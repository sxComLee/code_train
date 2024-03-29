/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.yarn.metrics.EventTypeMetrics;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Dispatches {@link Event}s in a separate thread. Currently only single thread
 * does that. Potentially there could be multiple channels for each event type
 * class and a thread pool can be used to dispatch the events.
 */
@SuppressWarnings("rawtypes")
@Public
@Evolving
public class AsyncDispatcher extends AbstractService implements Dispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncDispatcher.class);
    private static final Marker FATAL = MarkerFactory.getMarker("FATAL");

    // TODO_MA 马中华 注释： new LinkedBlockingQueue<Event>() 事件队列
    private final BlockingQueue<Event> eventQueue;

    private volatile int lastEventQueueSizeLogged = 0;
    private volatile int lastEventDetailsQueueSizeLogged = 0;
    private volatile boolean stopped = false;

    //Configuration for control the details queue event printing.
    private int detailsInterval;
    private boolean printTrigger = false;

    // Configuration flag for enabling/disabling draining dispatcher's events on
    // stop functionality.
    private volatile boolean drainEventsOnStop = false;

    // Indicates all the remaining dispatcher's events on stop have been drained
    // and processed.
    // Race condition happens if dispatcher thread sets drained to true between
    // handler setting drained to false and enqueueing event. YARN-3878 decided
    // to ignore it because of its tiny impact. Also see YARN-5436.
    private volatile boolean drained = true;
    private final Object waitForDrained = new Object();

    // For drainEventsOnStop enabled only, block newly coming events into the
    // queue while stopping.
    private volatile boolean blockNewEvents = false;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 负责提交事件到 AsyncDispatcher 内部的事件队列里面
     */
    private final EventHandler<Event> handlerInstance = new GenericEventHandler();

    // TODO_MA 马中华 注释： 消费线程
    private Thread eventHandlingThread;

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 事件 和 事件处理器 注册表
     */
    protected final Map<Class<? extends Enum>, EventHandler> eventDispatchers;
    private boolean exitOnDispatchException = true;

    private Map<Class<? extends Enum>, EventTypeMetrics> eventTypeMetricsMap;

    private Clock clock = new MonotonicClock();

    /**
     * The thread name for dispatcher.
     */
    private String dispatcherThreadName = "AsyncDispatcher event handler";

    public AsyncDispatcher() {
        this(new LinkedBlockingQueue<Event>());
    }

    public AsyncDispatcher(BlockingQueue<Event> eventQueue) {
        super("Dispatcher");
        this.eventQueue = eventQueue;
        this.eventDispatchers = new HashMap<Class<? extends Enum>, EventHandler>();
        this.eventTypeMetricsMap = new HashMap<Class<? extends Enum>, EventTypeMetrics>();
    }

    /**
     * Set a name for this dispatcher thread.
     * @param dispatcherName name of the dispatcher thread
     */
    public AsyncDispatcher(String dispatcherName) {
        this();
        dispatcherThreadName = dispatcherName;
    }

    /*************************************************
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： AsyncDispatcher 内部的 eventQueue 的消费线程
     */
    Runnable createThread() {
        return new Runnable() {
            @Override
            public void run() {
                while (!stopped && !Thread.currentThread().isInterrupted()) {
                    drained = eventQueue.isEmpty();
                    // blockNewEvents is only set when dispatcher is draining to stop,
                    // adding this check is to avoid the overhead of acquiring the lock
                    // and calling notify every time in the normal run of the loop.
                    if (blockNewEvents) {
                        synchronized (waitForDrained) {
                            if (drained) {
                                waitForDrained.notify();
                            }
                        }
                    }
                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 从队列取出事件进行消费
                     */
                    Event event;
                    try {
                        event = eventQueue.take();
                    } catch (InterruptedException ie) {
                        if (!stopped) {
                            LOG.warn("AsyncDispatcher thread interrupted", ie);
                        }
                        return;
                    }

                    /*************************************************
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 取出的事件不为空，则进行分发
                     */
                    if (event != null) {
                        if (eventTypeMetricsMap.get(event.getType().getDeclaringClass()) != null) {
                            long startTime = clock.getTime();
                            dispatch(event);
                            eventTypeMetricsMap.get(event.getType().getDeclaringClass())
                                    .increment(event.getType(), clock.getTime() - startTime);
                        } else {
                            /*************************************************
                             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                             *  注释： 做事件分发：
                             *  到 AsyncDispatcher 内部的注册表中，根据事件类型，找到对应的 EventHandler 来完成事件处理
                             */
                            dispatch(event);
                        }
                        if (printTrigger) {
                            //Log the latest dispatch event type
                            // may cause the too many events queued
                            LOG.info("Latest dispatch event type: " + event.getType());
                            printTrigger = false;
                        }
                    }
                }
            }
        };
    }

    @VisibleForTesting
    public void disableExitOnDispatchException() {
        exitOnDispatchException = false;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        this.detailsInterval = getConfig().getInt(YarnConfiguration.YARN_DISPATCHER_PRINT_EVENTS_INFO_THRESHOLD,
                YarnConfiguration.DEFAULT_YARN_DISPATCHER_PRINT_EVENTS_INFO_THRESHOLD
        );
    }

    @Override
    protected void serviceStart() throws Exception {
        //start all the components
        super.serviceStart();

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 进入到 createThread() 创建的线程的 run()
         */
        eventHandlingThread = new Thread(createThread());
        eventHandlingThread.setName(dispatcherThreadName);
        eventHandlingThread.start();
    }

    public void setDrainEventsOnStop() {
        drainEventsOnStop = true;
    }

    @Override
    protected void serviceStop() throws Exception {
        if (drainEventsOnStop) {
            blockNewEvents = true;
            LOG.info("AsyncDispatcher is draining to stop, ignoring any new events.");
            long endTime = System.currentTimeMillis() + getConfig().getLong(
                    YarnConfiguration.DISPATCHER_DRAIN_EVENTS_TIMEOUT,
                    YarnConfiguration.DEFAULT_DISPATCHER_DRAIN_EVENTS_TIMEOUT
            );

            synchronized (waitForDrained) {
                while (!isDrained() && eventHandlingThread != null && eventHandlingThread.isAlive() && System.currentTimeMillis() < endTime) {
                    waitForDrained.wait(100);
                    LOG.info("Waiting for AsyncDispatcher to drain. Thread state is :" + eventHandlingThread.getState());
                }
            }
        }
        stopped = true;
        if (eventHandlingThread != null) {
            eventHandlingThread.interrupt();
            try {
                eventHandlingThread.join();
            } catch (InterruptedException ie) {
                LOG.warn("Interrupted Exception while stopping", ie);
            }
        }

        // stop all the components
        super.serviceStop();
    }

    @SuppressWarnings("unchecked")
    protected void dispatch(Event event) {
        //all events go thru this loop
        LOG.debug("Dispatching the event {}.{}", event.getClass().getName(), event);

        Class<? extends Enum> type = event.getType().getDeclaringClass();

        try {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 从注册表中，根据事件类型，找到对应的 EventHandler
             */
            EventHandler handler = eventDispatchers.get(type);

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 调用 EventHandler 的 handle 方法执行处理
             */
            if (handler != null) {
                handler.handle(event);
            } else {
                throw new Exception("No handler for registered for " + type);
            }
        } catch (Throwable t) {
            //TODO Maybe log the state of the queue
            LOG.error(FATAL, "Error in dispatcher thread", t);
            // If serviceStop is called, we should exit this thread gracefully.
            if (exitOnDispatchException && (ShutdownHookManager.get().isShutdownInProgress()) == false && stopped == false) {
                stopped = true;
                Thread shutDownThread = new Thread(createShutDownThread());
                shutDownThread.setName("AsyncDispatcher ShutDown handler");
                shutDownThread.start();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void register(Class<? extends Enum> eventType, EventHandler handler) {
        /* check to see if we have a listener registered */
        EventHandler<Event> registeredHandler = (EventHandler<Event>) eventDispatchers.get(eventType);
        LOG.info("Registering " + eventType + " for " + handler.getClass());
        if (registeredHandler == null) {
            eventDispatchers.put(eventType, handler);
        } else if (!(registeredHandler instanceof MultiListenerHandler)) {
            /* for multiple listeners of an event add the multiple listener handler */
            MultiListenerHandler multiHandler = new MultiListenerHandler();
            multiHandler.addHandler(registeredHandler);
            multiHandler.addHandler(handler);
            eventDispatchers.put(eventType, multiHandler);
        } else {
            /* already a multilistener, just add to it */
            MultiListenerHandler multiHandler = (MultiListenerHandler) registeredHandler;
            multiHandler.addHandler(handler);
        }
    }

    @Override
    public EventHandler<Event> getEventHandler() {
        return handlerInstance;
    }

    class GenericEventHandler implements EventHandler<Event> {

        private void printEventQueueDetails() {
            Iterator<Event> iterator = eventQueue.iterator();
            Map<Enum, Long> counterMap = new HashMap<>();
            while (iterator.hasNext()) {
                Enum eventType = iterator.next().getType();
                if (!counterMap.containsKey(eventType)) {
                    counterMap.put(eventType, 0L);
                }
                counterMap.put(eventType, counterMap.get(eventType) + 1);
            }
            for (Map.Entry<Enum, Long> entry : counterMap.entrySet()) {
                long num = entry.getValue();
                LOG.info("Event type: " + entry.getKey() + ", Event record counter: " + num);
            }
        }

        public void handle(Event event) {
            if (blockNewEvents) {
                return;
            }
            drained = false;

            /* all this method does is enqueue all the events onto the queue */
            int qSize = eventQueue.size();
            if (qSize != 0 && qSize % 1000 == 0 && lastEventQueueSizeLogged != qSize) {
                lastEventQueueSizeLogged = qSize;
                LOG.info("Size of event-queue is " + qSize);
            }
            if (qSize != 0 && qSize % detailsInterval == 0 && lastEventDetailsQueueSizeLogged != qSize) {
                lastEventDetailsQueueSizeLogged = qSize;
                printEventQueueDetails();
                printTrigger = true;
            }
            int remCapacity = eventQueue.remainingCapacity();
            if (remCapacity < 1000) {
                LOG.warn("Very low remaining capacity in the event-queue: " + remCapacity);
            }
            try {
                /*************************************************
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： handle 的核心，就是丢 事件到 AsyncDispatcher 的 eventQueue 队列中
                 */
                eventQueue.put(event);
            } catch (InterruptedException e) {
                if (!stopped) {
                    LOG.warn("AsyncDispatcher thread interrupted", e);
                }
                // Need to reset drained flag to true if event queue is empty,
                // otherwise dispatcher will hang on stop.
                drained = eventQueue.isEmpty();
                throw new YarnRuntimeException(e);
            }
        }
    }

    /**
     * Multiplexing an event. Sending it to different handlers that
     * are interested in the event.
     * @param <T> the type of event these multiple handlers are interested in.
     */
    static class MultiListenerHandler implements EventHandler<Event> {
        List<EventHandler<Event>> listofHandlers;

        public MultiListenerHandler() {
            listofHandlers = new ArrayList<EventHandler<Event>>();
        }

        @Override
        public void handle(Event event) {
            for (EventHandler<Event> handler : listofHandlers) {
                handler.handle(event);
            }
        }

        void addHandler(EventHandler<Event> handler) {
            listofHandlers.add(handler);
        }

    }

    Runnable createShutDownThread() {
        return new Runnable() {
            @Override
            public void run() {
                LOG.info("Exiting, bbye..");
                System.exit(-1);
            }
        };
    }

    @VisibleForTesting
    protected boolean isEventThreadWaiting() {
        return eventHandlingThread.getState() == Thread.State.WAITING;
    }

    protected boolean isDrained() {
        return drained;
    }

    protected boolean isStopped() {
        return stopped;
    }

    public void addMetrics(EventTypeMetrics metrics, Class<? extends Enum> eventClass) {
        eventTypeMetricsMap.put(eventClass, metrics);
    }
}
