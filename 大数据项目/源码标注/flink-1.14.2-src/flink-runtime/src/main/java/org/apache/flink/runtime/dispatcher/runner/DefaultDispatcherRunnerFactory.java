/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.entrypoint.component.JobGraphRetriever;
import org.apache.flink.runtime.jobmanager.JobGraphStoreFactory;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.Executor;

/**
 * {@link DispatcherRunnerFactory} implementation which creates {@link DefaultDispatcherRunner}
 * instances.
 */
public class DefaultDispatcherRunnerFactory implements DispatcherRunnerFactory {

    // TODO_MA 马中华 注释： SessionDispatcherLeaderProcessFactoryFactory
    private final DispatcherLeaderProcessFactoryFactory dispatcherLeaderProcessFactoryFactory;

    public DefaultDispatcherRunnerFactory(DispatcherLeaderProcessFactoryFactory dispatcherLeaderProcessFactoryFactory) {
        this.dispatcherLeaderProcessFactoryFactory = dispatcherLeaderProcessFactoryFactory;
    }

    @Override
    public DispatcherRunner createDispatcherRunner(LeaderElectionService leaderElectionService,
                                                   FatalErrorHandler fatalErrorHandler,
                                                   JobGraphStoreFactory jobGraphStoreFactory,
                                                   Executor ioExecutor,
                                                   RpcService rpcService,
                                                   PartialDispatcherServices partialDispatcherServices) throws Exception {

        // TODO_MA 马中华 注释：
        final DispatcherLeaderProcessFactory dispatcherLeaderProcessFactory = dispatcherLeaderProcessFactoryFactory.createFactory(
                jobGraphStoreFactory,
                ioExecutor,
                rpcService,
                partialDispatcherServices,
                fatalErrorHandler
        );

        /*************************************************
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        return DefaultDispatcherRunner.create(leaderElectionService, fatalErrorHandler, dispatcherLeaderProcessFactory);
    }

    public static DefaultDispatcherRunnerFactory createSessionRunner(DispatcherFactory dispatcherFactory) {

        // TODO_MA 马中华 注释： 参数：SessionDispatcherLeaderProcessFactoryFactory
        return new DefaultDispatcherRunnerFactory(SessionDispatcherLeaderProcessFactoryFactory.create(dispatcherFactory));
    }

    public static DefaultDispatcherRunnerFactory createJobRunner(JobGraphRetriever jobGraphRetriever) {
        return new DefaultDispatcherRunnerFactory(JobDispatcherLeaderProcessFactoryFactory.create(jobGraphRetriever));
    }
}
