/*
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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The default implementation of {@link HeartbeatMonitor}.
 *
 * @param <O> Type of the payload being sent to the associated heartbeat target
 */
public class HeartbeatMonitorImpl<O> implements HeartbeatMonitor<O>, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatMonitorImpl.class);

    /** Resource ID of the monitored heartbeat target. */
    private final ResourceID resourceID;

    /** Associated heartbeat target. */
    // TODO_MA 马中华 注释： 心跳 HeartbeatTarget
    private final HeartbeatTarget<O> heartbeatTarget;

    private final ScheduledExecutor scheduledExecutor;

    /** Listener which is notified about heartbeat timeouts. */
    // TODO_MA 马中华 注释： 心跳 HeartbeatListener
    private final HeartbeatListener<?, ?> heartbeatListener;

    /** Maximum heartbeat timeout interval. */
    private final long heartbeatTimeoutIntervalMs;

    private final int failedRpcRequestsUntilUnreachable;

    private volatile ScheduledFuture<?> futureTimeout;

    private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);

    private final AtomicInteger numberFailedRpcRequestsSinceLastSuccess = new AtomicInteger(0);

    private volatile long lastHeartbeat;

    HeartbeatMonitorImpl(ResourceID resourceID,
                         HeartbeatTarget<O> heartbeatTarget,
                         ScheduledExecutor scheduledExecutor,
                         HeartbeatListener<?, O> heartbeatListener,
                         long heartbeatTimeoutIntervalMs,
                         int failedRpcRequestsUntilUnreachable) {

        this.resourceID = Preconditions.checkNotNull(resourceID);
        this.heartbeatTarget = Preconditions.checkNotNull(heartbeatTarget);
        this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);
        this.heartbeatListener = Preconditions.checkNotNull(heartbeatListener);

        Preconditions.checkArgument(heartbeatTimeoutIntervalMs > 0L,
                "The heartbeat timeout interval has to be larger than 0."
        );
        this.heartbeatTimeoutIntervalMs = heartbeatTimeoutIntervalMs;

        Preconditions.checkArgument(failedRpcRequestsUntilUnreachable > 0 || failedRpcRequestsUntilUnreachable == -1,
                "The number of failed heartbeat RPC requests has to be larger than 0 or -1 (deactivated)."
        );
        this.failedRpcRequestsUntilUnreachable = failedRpcRequestsUntilUnreachable;

        lastHeartbeat = 0L;

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        resetHeartbeatTimeout(heartbeatTimeoutIntervalMs);
    }

    @Override
    public HeartbeatTarget<O> getHeartbeatTarget() {
        return heartbeatTarget;
    }

    @Override
    public ResourceID getHeartbeatTargetId() {
        return resourceID;
    }

    @Override
    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    @Override
    public void reportHeartbeatRpcFailure() {
        final int failedRpcRequestsSinceLastSuccess = numberFailedRpcRequestsSinceLastSuccess.incrementAndGet();

        if (isHeartbeatRpcFailureDetectionEnabled()
                && failedRpcRequestsSinceLastSuccess >= failedRpcRequestsUntilUnreachable) {
            if (state.compareAndSet(State.RUNNING, State.UNREACHABLE)) {
                LOG.debug("Mark heartbeat target {} as unreachable because {} consecutive heartbeat RPCs have failed.",
                        resourceID,
                        failedRpcRequestsSinceLastSuccess
                );

                cancelTimeout();
                heartbeatListener.notifyTargetUnreachable(resourceID);
            }
        }
    }

    private boolean isHeartbeatRpcFailureDetectionEnabled() {
        return failedRpcRequestsUntilUnreachable > 0;
    }

    @Override
    public void reportHeartbeatRpcSuccess() {
        numberFailedRpcRequestsSinceLastSuccess.set(0);
    }

    @Override
    public void reportHeartbeat() {
        lastHeartbeat = System.currentTimeMillis();
        resetHeartbeatTimeout(heartbeatTimeoutIntervalMs);
    }

    @Override
    public void cancel() {
        // we can only cancel if we are in state running
        if (state.compareAndSet(State.RUNNING, State.CANCELED)) {
            cancelTimeout();
        }
    }

    @Override
    public void run() {
        // The heartbeat has timed out if we're in state running
        if (state.compareAndSet(State.RUNNING, State.TIMEOUT)) {
            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 心跳超时处理
             *  Flink 中的 ResourceManager 和 TaskExecutor 和 JobMaster 的两两之间，都存在心跳
             */
            heartbeatListener.notifyHeartbeatTimeout(resourceID);
        }
    }

    public boolean isCanceled() {
        return state.get() == State.CANCELED;
    }

    void resetHeartbeatTimeout(long heartbeatTimeout) {
        if (state.get() == State.RUNNING) {

            // TODO_MA 马中华 注释： 取消超时任务
            cancelTimeout();

            /*************************************************
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动一个定时任务
             */
            futureTimeout = scheduledExecutor.schedule(this, heartbeatTimeout, TimeUnit.MILLISECONDS);

            // Double check for concurrent accesses (e.g. a firing of the scheduled future)
            if (state.get() != State.RUNNING) {
                cancelTimeout();
            }
        }
    }

    private void cancelTimeout() {
        if (futureTimeout != null) {
            futureTimeout.cancel(true);
        }
    }

    private enum State {
        RUNNING,
        TIMEOUT,
        UNREACHABLE,
        CANCELED
    }

    /**
     * The factory that instantiates {@link HeartbeatMonitorImpl}.
     *
     * @param <O> Type of the outgoing heartbeat payload
     */
    static class Factory<O> implements HeartbeatMonitor.Factory<O> {

        @Override
        public HeartbeatMonitor<O> createHeartbeatMonitor(ResourceID resourceID,
                                                          HeartbeatTarget<O> heartbeatTarget,
                                                          ScheduledExecutor mainThreadExecutor,
                                                          HeartbeatListener<?, O> heartbeatListener,
                                                          long heartbeatTimeoutIntervalMs,
                                                          int failedRpcRequestsUntilUnreachable) {

            return new HeartbeatMonitorImpl<>(resourceID,
                    heartbeatTarget,
                    mainThreadExecutor,
                    heartbeatListener,
                    heartbeatTimeoutIntervalMs,
                    failedRpcRequestsUntilUnreachable
            );
        }
    }
}
