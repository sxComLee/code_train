/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.controller

import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.CoreUtils.inLock
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.utils.Time

import scala.collection._

object ControllerEventManager {
    val ControllerEventThreadName = "controller-event-thread"
    val EventQueueTimeMetricName = "EventQueueTimeMs"
    val EventQueueSizeMetricName = "EventQueueSize"
}

trait ControllerEventProcessor {
    def process(event: ControllerEvent): Unit

    def preempt(event: ControllerEvent): Unit
}

class QueuedEvent(val event: ControllerEvent,
    val enqueueTimeMs: Long
) {
    val processingStarted = new CountDownLatch(1)
    val spent = new AtomicBoolean(false)

    def process(processor: ControllerEventProcessor): Unit = {
        if (spent.getAndSet(true))
            return
        processingStarted.countDown()

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： processor = KafkaController
         */
        processor.process(event)
    }

    def preempt(processor: ControllerEventProcessor): Unit = {
        if (spent.getAndSet(true))
            return
        processor.preempt(event)
    }

    def awaitProcessing(): Unit = {
        processingStarted.await()
    }

    override def toString: String = {
        s"QueuedEvent(event=$event, enqueueTimeMs=$enqueueTimeMs)"
    }
}

class ControllerEventManager(controllerId: Int,
    // TODO_MA 马中华 注释： processor  = KafkaController
    processor: ControllerEventProcessor,
    time: Time,
    rateAndTimeMetrics: Map[ControllerState, KafkaTimer],
    eventQueueTimeTimeoutMs: Long = 300000
) extends KafkaMetricsGroup {

    import ControllerEventManager._

    @volatile private var _state: ControllerState = ControllerState.Idle
    private val putLock = new ReentrantLock()

    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 事件队列
     */
    private val queue = new LinkedBlockingQueue[QueuedEvent]

    // Visible for test
    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 队列消费线程
     */
    private[controller] var thread = new ControllerEventThread(ControllerEventThreadName)

    private val eventQueueTimeHist = newHistogram(EventQueueTimeMetricName)

    newGauge(EventQueueSizeMetricName, () => queue.size)

    def state: ControllerState = _state

    // TODO_MA 马中华 注释： 线程启动
    def start(): Unit = thread.start()

    def close(): Unit = {
        try {
            thread.initiateShutdown()
            clearAndPut(ShutdownEventThread)
            thread.awaitShutdown()
        } finally {
            removeMetric(EventQueueTimeMetricName)
            removeMetric(EventQueueSizeMetricName)
        }
    }

    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    def put(event: ControllerEvent): QueuedEvent = inLock(putLock) {
        val queuedEvent = new QueuedEvent(event, time.milliseconds())
        queue.put(queuedEvent)
        queuedEvent
    }

    def clearAndPut(event: ControllerEvent): QueuedEvent = inLock(putLock) {
        val preemptedEvents = new ArrayList[QueuedEvent]()
        queue.drainTo(preemptedEvents)
        preemptedEvents.forEach(_.preempt(processor))
        put(event)
    }

    def isEmpty: Boolean = queue.isEmpty

    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： ShutdownableThread 的定义中，把核心业务逻辑迁移到了 doWork() (这个方法就是 run() 的内部逻辑)
     */
    class ControllerEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
        logIdent = s"[ControllerEventThread controllerId=$controllerId] "

        override def doWork(): Unit = {

            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 从队列中取出 事件
             */
            val dequeued = pollFromEventQueue()

            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 对事件执行消费
             */
            dequeued.event match {
                case ShutdownEventThread => // The shutting down of the thread has been initiated at this point. Ignore this event.
                case controllerEvent =>
                    _state = controllerEvent.state
                    eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)
                    try {
                        /**
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释：
                         *  1、dequeued.process(processor)
                         *  2、processor.process(dequeued)
                         */
                        def process(): Unit = dequeued.process(processor)

                        /**
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释：
                         */
                        rateAndTimeMetrics.get(state) match {
                            case Some(timer) => timer.time { process() }
                            case None => process()
                        }
                    } catch {
                        case e: Throwable => error(s"Uncaught error processing event $controllerEvent", e)
                    }

                    _state = ControllerState.Idle
            }
        }
    }

    private def pollFromEventQueue(): QueuedEvent = {
        val count = eventQueueTimeHist.count()
        if (count != 0) {
            val event = queue.poll(eventQueueTimeTimeoutMs, TimeUnit.MILLISECONDS)
            if (event == null) {
                eventQueueTimeHist.clear()
                queue.take()
            } else {
                event
            }
        } else {
            queue.take()
        }
    }

}
