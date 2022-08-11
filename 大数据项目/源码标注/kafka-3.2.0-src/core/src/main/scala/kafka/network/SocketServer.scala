/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.io.IOException
import java.net._
import java.nio.ByteBuffer
import java.nio.channels.{Selector => NSelector, _}
import java.util
import java.util.Optional
import java.util.concurrent._
import java.util.concurrent.atomic._

import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.metrics.KafkaMetricsGroup
import kafka.network.ConnectionQuotas._
import kafka.network.Processor._
import kafka.network.RequestChannel.{CloseConnectionResponse, EndThrottlingResponse, NoOpResponse, SendResponse, StartThrottlingResponse}
import kafka.network.SocketServer._
import kafka.security.CredentialProvider
import kafka.server.{ApiVersionManager, BrokerReconfigurable, KafkaConfig}
import kafka.utils.Implicits._
import kafka.utils._
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.internals.QuotaConfigs
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.memory.{MemoryPool, SimpleMemoryPool}
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.{Avg, CumulativeSum, Meter, Rate}
import org.apache.kafka.common.network.KafkaChannel.ChannelMuteEvent
import org.apache.kafka.common.network.{ChannelBuilder, ChannelBuilders, ClientInformation, KafkaChannel, ListenerName, ListenerReconfigurable, NetworkSend, Selectable, Send, Selector => KSelector}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{ApiVersionsRequest, RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time, Utils}
import org.apache.kafka.common.{Endpoint, KafkaException, MetricName, Reconfigurable}
import org.slf4j.event.Level

import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.ControlThrowable

/**
 * Handles new connections, requests and responses to and from broker.
 * Kafka supports two types of request planes :
 *  - data-plane :
 *    - Handles requests from clients and other brokers in the cluster.
 *    - The threading model is
 *      1 Acceptor thread per listener, that handles new connections.
 *      It is possible to configure multiple data-planes by specifying multiple "," separated endpoints for "listeners" in KafkaConfig.
 *      Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *      M Handler threads that handle requests and produce responses back to the processor threads for writing.
 *  - control-plane :
 *    - Handles requests from controller. This is optional and can be configured by specifying "control.plane.listener.name".
 *      If not configured, the controller requests are handled by the data-plane.
 *    - The threading model is
 *      1 Acceptor thread that handles new connections
 *      Acceptor has 1 Processor thread that has its own selector and read requests from the socket.
 *      1 Handler thread that handles requests and produces responses back to the processor thread for writing.
 */
class SocketServer(val config: KafkaConfig,
    val metrics: Metrics,
    val time: Time,
    val credentialProvider: CredentialProvider,
    val apiVersionManager: ApiVersionManager
)
    extends Logging with KafkaMetricsGroup with BrokerReconfigurable {

    private val maxQueuedRequests = config.queuedMaxRequests

    // TODO_MA 马中华 注释： broker ID
    protected val nodeId = config.brokerId

    private val logContext = new LogContext(s"[SocketServer listenerType=${apiVersionManager.listenerType}, nodeId=$nodeId] ")

    this.logIdent = logContext.logPrefix

    private val memoryPoolSensor = metrics.sensor("MemoryPoolUtilization")
    private val memoryPoolDepletedPercentMetricName = metrics.metricName("MemoryPoolAvgDepletedPercent", MetricsGroup)
    private val memoryPoolDepletedTimeMetricName = metrics.metricName("MemoryPoolDepletedTimeTotal", MetricsGroup)
    memoryPoolSensor.add(new Meter(TimeUnit.MILLISECONDS, memoryPoolDepletedPercentMetricName, memoryPoolDepletedTimeMetricName))
    private val memoryPool = if (config.queuedMaxBytes > 0) new SimpleMemoryPool(config.queuedMaxBytes, config.socketRequestMaxBytes, false, memoryPoolSensor) else MemoryPool.NONE


    // data-plane
    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： dataPlane Acceptor 组件
     */
    private[network] val dataPlaneAcceptors = new ConcurrentHashMap[EndPoint, DataPlaneAcceptor]()
    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： dataPlane RequestChannel 组件
     */
    val dataPlaneRequestChannel = new RequestChannel(maxQueuedRequests, DataPlaneAcceptor.MetricPrefix, time, apiVersionManager.newRequestMetrics)


    // control-plane
    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： controllerPlane Accptor
     */
    private[network] var controlPlaneAcceptorOpt: Option[ControlPlaneAcceptor] = None
    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： controllerPlane RequestChannel
     */
    val controlPlaneRequestChannelOpt: Option[RequestChannel] = config.controlPlaneListenerName.map(_ =>
        new RequestChannel(20, ControlPlaneAcceptor.MetricPrefix, time, apiVersionManager.newRequestMetrics))

    private[this] val nextProcessorId: AtomicInteger = new AtomicInteger(0)
    val connectionQuotas = new ConnectionQuotas(config, time, metrics)

    private var startedProcessingRequests = false
    private var stoppedProcessingRequests = false

    // Processors are now created by each Acceptor. However to preserve compatibility, we need to number the processors
    // globally, so we keep the nextProcessorId counter in SocketServer
    def nextProcessorId(): Int = {
        nextProcessorId.getAndIncrement()
    }

    /**
     * Starts the socket server and creates all the Acceptors and the Processors. The Acceptors
     * start listening at this stage so that the bound port is known when this method completes
     * even when ephemeral ports are used. Acceptors and Processors are started if `startProcessingRequests`
     * is true. If not, acceptors and processors are only started when [[kafka.network.SocketServer#startProcessingRequests()]]
     * is invoked. Delayed starting of acceptors and processors is used to delay processing client
     * connections until server is fully initialized, e.g. to ensure that all credentials have been
     * loaded before authentications are performed. Incoming connections on this server are processed
     * when processors start up and invoke [[org.apache.kafka.common.network.Selector#poll]].
     *
     * @param startProcessingRequests Flag indicating whether `Processor`s must be started.
     * @param controlPlaneListener    The control plane listener, or None if there is none.
     * @param dataPlaneListeners      The data plane listeners.
     */
    def startup(startProcessingRequests: Boolean = true,
        controlPlaneListener: Option[EndPoint] = config.controlPlaneListener,
        dataPlaneListeners: Seq[EndPoint] = config.dataPlaneListeners
    ): Unit = {
        this.synchronized {

            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 创建 ControlPlane 级别的 一个 Acceptor 和一组 Processor
             */
            createControlPlaneAcceptorAndProcessor(controlPlaneListener)

            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 创建 DataPlane 级别的 一个 Acceptor 和一组 Processor
             */
            createDataPlaneAcceptorsAndProcessors(dataPlaneListeners)

            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动 ControlPlane 和 DataPlane 级别的 Processor 和 Acceptor
             */
            if (startProcessingRequests) {
                this.startProcessingRequests()
            }
        }

        val dataPlaneProcessors = dataPlaneAcceptors.asScala.values.flatMap(a => a.processors)
        val controlPlaneProcessorOpt = controlPlaneAcceptorOpt.map(a => a.processors(0))
        newGauge(s"${DataPlaneAcceptor.MetricPrefix}NetworkProcessorAvgIdlePercent", () => SocketServer.this.synchronized {
            val ioWaitRatioMetricNames = dataPlaneProcessors.map { p =>
                metrics.metricName("io-wait-ratio", MetricsGroup, p.metricTags)
            }
            ioWaitRatioMetricNames.map { metricName =>
                Option(metrics.metric(metricName)).fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
            }.sum / dataPlaneProcessors.size
        })
        newGauge(s"${ControlPlaneAcceptor.MetricPrefix}NetworkProcessorAvgIdlePercent", () => SocketServer.this.synchronized {
            val ioWaitRatioMetricName = controlPlaneProcessorOpt.map { p =>
                metrics.metricName("io-wait-ratio", MetricsGroup, p.metricTags)
            }
            ioWaitRatioMetricName.map { metricName =>
                Option(metrics.metric(metricName)).fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
            }.getOrElse(Double.NaN)
        })
        newGauge("MemoryPoolAvailable", () => memoryPool.availableMemory)
        newGauge("MemoryPoolUsed", () => memoryPool.size() - memoryPool.availableMemory)
        newGauge(s"${DataPlaneAcceptor.MetricPrefix}ExpiredConnectionsKilledCount", () => SocketServer.this.synchronized {
            val expiredConnectionsKilledCountMetricNames = dataPlaneProcessors.map { p =>
                metrics.metricName("expired-connections-killed-count", MetricsGroup, p.metricTags)
            }
            expiredConnectionsKilledCountMetricNames.map { metricName =>
                Option(metrics.metric(metricName)).fold(0.0)(m => m.metricValue.asInstanceOf[Double])
            }.sum
        })
        newGauge(s"${ControlPlaneAcceptor.MetricPrefix}ExpiredConnectionsKilledCount", () => SocketServer.this.synchronized {
            val expiredConnectionsKilledCountMetricNames = controlPlaneProcessorOpt.map { p =>
                metrics.metricName("expired-connections-killed-count", MetricsGroup, p.metricTags)
            }
            expiredConnectionsKilledCountMetricNames.map { metricName =>
                Option(metrics.metric(metricName)).fold(0.0)(m => m.metricValue.asInstanceOf[Double])
            }.getOrElse(0.0)
        })
    }

    /**
     * Start processing requests and new connections. This method is used for delayed starting of
     * all the acceptors and processors if [[kafka.network.SocketServer#startup]] was invoked with
     * `startProcessingRequests=false`.
     *
     * Before starting processors for each endpoint, we ensure that authorizer has all the metadata
     * to authorize requests on that endpoint by waiting on the provided future. We start inter-broker
     * listener before other listeners. This allows authorization metadata for other listeners to be
     * stored in Kafka topics in this cluster.
     *
     * @param authorizerFutures Future per [[EndPoint]] used to wait before starting the processor
     *                          corresponding to the [[EndPoint]]
     */
    def startProcessingRequests(authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = Map.empty): Unit = {
        info("Starting socket server acceptors and processors")
        this.synchronized {
            if (!startedProcessingRequests) {

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 启动 ControlPlane 级别的 Acceptor 和 Processor
                 */
                startControlPlaneProcessorAndAcceptor(authorizerFutures)

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 启动 ControlPlane 级别的 Acceptor 和 Processor
                 */
                startDataPlaneProcessorsAndAcceptors(authorizerFutures)

                // TODO_MA 马中华 注释： 标记启动完成
                startedProcessingRequests = true
            } else {
                info("Socket server acceptors and processors already started")
            }
        }
        info("Started socket server acceptors and processors")
    }

    /**
     * Starts processors of the provided acceptor and the acceptor itself.
     *
     * Before starting them, we ensure that authorizer has all the metadata to authorize
     * requests on that endpoint by waiting on the provided future.
     */
    private def startAcceptorAndProcessors(acceptor: Acceptor,
        authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = Map.empty
    ): Unit = {
        val endpoint = acceptor.endPoint
        debug(s"Wait for authorizer to complete start up on listener ${endpoint.listenerName}")
        waitForAuthorizerFuture(acceptor, authorizerFutures)
        debug(s"Start processors on listener ${endpoint.listenerName}")

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动 Processor
         */
        acceptor.startProcessors()
        debug(s"Start acceptor thread on listener ${endpoint.listenerName}")

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动 Acceptor
         */
        if (!acceptor.isStarted()) {
            KafkaThread.nonDaemon(
                s"${acceptor.threadPrefix()}-kafka-socket-acceptor-${endpoint.listenerName}-${endpoint.securityProtocol}-${endpoint.port}",
                acceptor
            ).start()
            acceptor.awaitStartup()
        }
        info(s"Started ${acceptor.threadPrefix()} acceptor and processor(s) for endpoint : ${endpoint.listenerName}")
    }

    /**
     * Starts processors of all the data-plane acceptors and all the acceptors of this server.
     *
     * We start inter-broker listener before other listeners. This allows authorization metadata for
     * other listeners to be stored in Kafka topics in this cluster.
     */
    private def startDataPlaneProcessorsAndAcceptors(authorizerFutures: Map[Endpoint, CompletableFuture[Void]]): Unit = {

        // TODO_MA 马中华 注释：
        val interBrokerListener = dataPlaneAcceptors.asScala.keySet
            .find(_.listenerName == config.interBrokerListenerName)

        // TODO_MA 马中华 注释：
        val orderedAcceptors = interBrokerListener match {
            case Some(interBrokerListener) => List(dataPlaneAcceptors.get(interBrokerListener)) ++
                dataPlaneAcceptors.asScala.filter { case (k, _) => k != interBrokerListener }.values
            case None => dataPlaneAcceptors.asScala.values
        }

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动 Acceptor 和 Processor
         */
        orderedAcceptors.foreach { acceptor =>
            startAcceptorAndProcessors(acceptor, authorizerFutures)
        }
    }

    /**
     * Start the processor of control-plane acceptor and the acceptor of this server.
     */
    private def startControlPlaneProcessorAndAcceptor(authorizerFutures: Map[Endpoint, CompletableFuture[Void]]): Unit = {

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        controlPlaneAcceptorOpt.foreach { controlPlaneAcceptor =>

            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            startAcceptorAndProcessors(controlPlaneAcceptor, authorizerFutures)
        }
    }

    private def endpoints = config.listeners.map(l => l.listenerName -> l).toMap

    def createDataPlaneAcceptorsAndProcessors(endpoints: Seq[EndPoint]): Unit = {

        // TODO_MA 马中华 注释：
        endpoints.foreach { endpoint =>
            val parsedConfigs = config.valuesFromThisConfigWithPrefixOverride(endpoint.listenerName.configPrefix)
            connectionQuotas.addListener(config, endpoint.listenerName)

            val isPrivilegedListener = controlPlaneRequestChannelOpt.isEmpty && config.interBrokerListenerName == endpoint.listenerName

            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 创建 DataPlane 级别的 Acceptor
             */
            val dataPlaneAcceptor = createDataPlaneAcceptor(endpoint, isPrivilegedListener, dataPlaneRequestChannel)

            config.addReconfigurable(dataPlaneAcceptor)

            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 创建 DataPlane 级别的 Processor
             */
            dataPlaneAcceptor.configure(parsedConfigs)
            dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)

            info(s"Created data-plane acceptor and processors for endpoint : ${endpoint.listenerName}")
        }
    }

    private def createControlPlaneAcceptorAndProcessor(endpointOpt: Option[EndPoint]): Unit = {

        // TODO_MA 马中华 注释：
        endpointOpt.foreach { endpoint =>
            connectionQuotas.addListener(config, endpoint.listenerName)

            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 创建 Acceptor
             */
            val controlPlaneAcceptor = createControlPlaneAcceptor(endpoint, controlPlaneRequestChannelOpt.get)

            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 创建和启动 Processor
             */
            controlPlaneAcceptor.addProcessors(1)
            controlPlaneAcceptorOpt = Some(controlPlaneAcceptor)
            info(s"Created control-plane acceptor and processor for endpoint : ${endpoint.listenerName}")
        }
    }


    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    protected def createDataPlaneAcceptor(endPoint: EndPoint, isPrivilegedListener: Boolean, requestChannel: RequestChannel): DataPlaneAcceptor = {
        new DataPlaneAcceptor(this, endPoint, config, nodeId, connectionQuotas, time, isPrivilegedListener, requestChannel, metrics, credentialProvider, logContext, memoryPool, apiVersionManager)
    }

    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    private def createControlPlaneAcceptor(endPoint: EndPoint, requestChannel: RequestChannel): ControlPlaneAcceptor = {
        new ControlPlaneAcceptor(this, endPoint, config, nodeId, connectionQuotas, time, requestChannel, metrics, credentialProvider, logContext, memoryPool, apiVersionManager)
    }

    /**
     * Stop processing requests and new connections.
     */
    def stopProcessingRequests(): Unit = {
        info("Stopping socket server request processors")
        this.synchronized {
            dataPlaneAcceptors.asScala.values.foreach(_.initiateShutdown())
            dataPlaneAcceptors.asScala.values.foreach(_.awaitShutdown())
            controlPlaneAcceptorOpt.foreach(_.initiateShutdown())
            controlPlaneAcceptorOpt.foreach(_.awaitShutdown())
            dataPlaneRequestChannel.clear()
            controlPlaneRequestChannelOpt.foreach(_.clear())
            stoppedProcessingRequests = true
        }
        info("Stopped socket server request processors")
    }

    /**
     * Shutdown the socket server. If still processing requests, shutdown
     * acceptors and processors first.
     */
    def shutdown(): Unit = {
        info("Shutting down socket server")
        this.synchronized {
            if (!stoppedProcessingRequests)
                stopProcessingRequests()
            dataPlaneRequestChannel.shutdown()
            controlPlaneRequestChannelOpt.foreach(_.shutdown())
            connectionQuotas.close()
        }
        info("Shutdown completed")
    }

    def boundPort(listenerName: ListenerName): Int = {
        try {
            val acceptor = dataPlaneAcceptors.get(endpoints(listenerName))
            if (acceptor != null) {
                acceptor.serverChannel.socket.getLocalPort
            } else {
                controlPlaneAcceptorOpt.map(_.serverChannel.socket().getLocalPort).getOrElse(throw new KafkaException("Could not find listenerName : " + listenerName + " in data-plane or control-plane"))
            }
        } catch {
            case e: Exception =>
                throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
        }
    }

    def addListeners(listenersAdded: Seq[EndPoint]): Unit = synchronized {
        info(s"Adding data-plane listeners for endpoints $listenersAdded")
        createDataPlaneAcceptorsAndProcessors(listenersAdded)
        listenersAdded.foreach { endpoint =>
            val acceptor = dataPlaneAcceptors.get(endpoint)
            startAcceptorAndProcessors(acceptor)
        }
    }

    def removeListeners(listenersRemoved: Seq[EndPoint]): Unit = synchronized {
        info(s"Removing data-plane listeners for endpoints $listenersRemoved")
        listenersRemoved.foreach { endpoint =>
            connectionQuotas.removeListener(config, endpoint.listenerName)
            dataPlaneAcceptors.asScala.remove(endpoint).foreach { acceptor =>
                acceptor.initiateShutdown()
                acceptor.awaitShutdown()
            }
        }
    }

    override def reconfigurableConfigs: Set[String] = SocketServer.ReconfigurableConfigs

    override def validateReconfiguration(newConfig: KafkaConfig): Unit = {

    }

    override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
        val maxConnectionsPerIp = newConfig.maxConnectionsPerIp
        if (maxConnectionsPerIp != oldConfig.maxConnectionsPerIp) {
            info(s"Updating maxConnectionsPerIp: $maxConnectionsPerIp")
            connectionQuotas.updateMaxConnectionsPerIp(maxConnectionsPerIp)
        }
        val maxConnectionsPerIpOverrides = newConfig.maxConnectionsPerIpOverrides
        if (maxConnectionsPerIpOverrides != oldConfig.maxConnectionsPerIpOverrides) {
            info(s"Updating maxConnectionsPerIpOverrides: ${maxConnectionsPerIpOverrides.map { case (k, v) => s"$k=$v" }.mkString(",")}")
            connectionQuotas.updateMaxConnectionsPerIpOverride(maxConnectionsPerIpOverrides)
        }
        val maxConnections = newConfig.maxConnections
        if (maxConnections != oldConfig.maxConnections) {
            info(s"Updating broker-wide maxConnections: $maxConnections")
            connectionQuotas.updateBrokerMaxConnections(maxConnections)
        }
        val maxConnectionRate = newConfig.maxConnectionCreationRate
        if (maxConnectionRate != oldConfig.maxConnectionCreationRate) {
            info(s"Updating broker-wide maxConnectionCreationRate: $maxConnectionRate")
            connectionQuotas.updateBrokerMaxConnectionRate(maxConnectionRate)
        }
    }

    private def waitForAuthorizerFuture(acceptor: Acceptor,
        authorizerFutures: Map[Endpoint, CompletableFuture[Void]]
    ): Unit = {
        //we can't rely on authorizerFutures.get() due to ephemeral ports. Get the future using listener name
        authorizerFutures.forKeyValue { (endpoint, future) =>
            if (endpoint.listenerName == Optional.of(acceptor.endPoint.listenerName.value))
                future.join()
        }
    }

    // For test usage
    private[network] def connectionCount(address: InetAddress): Int =
        Option(connectionQuotas).fold(0)(_.get(address))

    // For test usage
    def dataPlaneAcceptor(listenerName: String): Option[DataPlaneAcceptor] = {
        dataPlaneAcceptors.asScala.foreach { case (endPoint, acceptor) =>
            if (endPoint.listenerName.value() == listenerName)
                return Some(acceptor)
        }
        None
    }
}

object SocketServer {
    val MetricsGroup = "socket-server-metrics"

    val ReconfigurableConfigs = Set(
        KafkaConfig.MaxConnectionsPerIpProp,
        KafkaConfig.MaxConnectionsPerIpOverridesProp,
        KafkaConfig.MaxConnectionsProp,
        KafkaConfig.MaxConnectionCreationRateProp)

    val ListenerReconfigurableConfigs = Set(KafkaConfig.MaxConnectionsProp, KafkaConfig.MaxConnectionCreationRateProp)
}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

    private val startupLatch = new CountDownLatch(1)

    // `shutdown()` is invoked before `startupComplete` and `shutdownComplete` if an exception is thrown in the constructor
    // (e.g. if the address is already in use). We want `shutdown` to proceed in such cases, so we first assign an open
    // latch and then replace it in `startupComplete()`.
    @volatile private var shutdownLatch = new CountDownLatch(0)

    private val alive = new AtomicBoolean(true)

    def wakeup(): Unit

    /**
     * Initiates a graceful shutdown by signaling to stop
     */
    def initiateShutdown(): Unit = {
        if (alive.getAndSet(false))
            wakeup()
    }

    /**
     * Wait for the thread to completely shutdown
     */
    def awaitShutdown(): Unit = shutdownLatch.await

    /**
     * Returns true if the thread is completely started
     */
    def isStarted(): Boolean = startupLatch.getCount == 0

    /**
     * Wait for the thread to completely start up
     */
    def awaitStartup(): Unit = startupLatch.await

    /**
     * Record that the thread startup is complete
     */
    protected def startupComplete(): Unit = {
        // Replace the open latch with a closed one
        shutdownLatch = new CountDownLatch(1)
        startupLatch.countDown()
    }

    /**
     * Record that the thread shutdown is complete
     */
    protected def shutdownComplete(): Unit = shutdownLatch.countDown()

    /**
     * Is the server still running?
     */
    protected def isRunning: Boolean = alive.get

    /**
     * Close `channel` and decrement the connection count.
     */
    def close(listenerName: ListenerName, channel: SocketChannel): Unit = {
        if (channel != null) {
            debug(s"Closing connection from ${channel.socket.getRemoteSocketAddress}")
            connectionQuotas.dec(listenerName, channel.socket.getInetAddress)
            closeSocket(channel)
        }
    }

    protected def closeSocket(channel: SocketChannel): Unit = {
        CoreUtils.swallow(channel.socket().close(), this, Level.ERROR)
        CoreUtils.swallow(channel.close(), this, Level.ERROR)
    }
}

object DataPlaneAcceptor {
    val ThreadPrefix = "data-plane"
    val MetricPrefix = ""
    val ListenerReconfigurableConfigs = Set(KafkaConfig.NumNetworkThreadsProp)
}

class DataPlaneAcceptor(socketServer: SocketServer,
    endPoint: EndPoint,
    config: KafkaConfig,
    nodeId: Int,
    connectionQuotas: ConnectionQuotas,
    time: Time,
    isPrivilegedListener: Boolean,
    requestChannel: RequestChannel,
    metrics: Metrics,
    credentialProvider: CredentialProvider,
    logContext: LogContext,
    memoryPool: MemoryPool,
    apiVersionManager: ApiVersionManager
)
    extends Acceptor(socketServer,
        endPoint,
        config,
        nodeId,
        connectionQuotas,
        time,
        isPrivilegedListener,
        requestChannel,
        metrics,
        credentialProvider,
        logContext,
        memoryPool,
        apiVersionManager) with ListenerReconfigurable {

    override def metricPrefix(): String = DataPlaneAcceptor.MetricPrefix

    override def threadPrefix(): String = DataPlaneAcceptor.ThreadPrefix

    /**
     * Returns the listener name associated with this reconfigurable. Listener-specific
     * configs corresponding to this listener name are provided for reconfiguration.
     */
    override def listenerName(): ListenerName = endPoint.listenerName

    /**
     * Returns the names of configs that may be reconfigured.
     */
    override def reconfigurableConfigs(): util.Set[String] = DataPlaneAcceptor.ListenerReconfigurableConfigs.asJava


    /**
     * Validates the provided configuration. The provided map contains
     * all configs including any reconfigurable configs that may be different
     * from the initial configuration. Reconfiguration will be not performed
     * if this method throws any exception.
     *
     * @throws ConfigException if the provided configs are not valid. The exception
     *                         message from ConfigException will be returned to the client in
     *                         the AlterConfigs response.
     */
    override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
        configs.forEach { (k, v) =>
            if (reconfigurableConfigs.contains(k)) {
                val newValue = v.asInstanceOf[Int]
                val oldValue = processors.length
                if (newValue != oldValue) {
                    val errorMsg = s"Dynamic thread count update validation failed for $k=$v"
                    if (newValue <= 0)
                        throw new ConfigException(s"$errorMsg, value should be at least 1")
                    if (newValue < oldValue / 2)
                        throw new ConfigException(s"$errorMsg, value should be at least half the current value $oldValue")
                    if (newValue > oldValue * 2)
                        throw new ConfigException(s"$errorMsg, value should not be greater than double the current value $oldValue")
                }
            }
        }
    }

    /**
     * Reconfigures this instance with the given key-value pairs. The provided
     * map contains all configs including any reconfigurable configs that
     * may have changed since the object was initially configured using
     * {@link Configurable# configure ( Map )}. This method will only be invoked if
     * the configs have passed validation using {@link #validateReconfiguration ( Map )}.
     */
    override def reconfigure(configs: util.Map[String, _]): Unit = {
        val newNumNetworkThreads = configs.get(KafkaConfig.NumNetworkThreadsProp).asInstanceOf[Int]

        if (newNumNetworkThreads != processors.length) {
            info(s"Resizing network thread pool size for ${endPoint.listenerName} listener from ${processors.length} to $newNumNetworkThreads")
            if (newNumNetworkThreads > processors.length) {
                addProcessors(newNumNetworkThreads - processors.length)
            } else if (newNumNetworkThreads < processors.length) {
                removeProcessors(processors.length - newNumNetworkThreads)
            }
        }
    }

    /**
     * Configure this class with the given key-value pairs
     */
    override def configure(configs: util.Map[String, _]): Unit = {

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： num.network.threads 表示了线程个数
         */
        addProcessors(configs.get(KafkaConfig.NumNetworkThreadsProp).asInstanceOf[Int])
    }
}

object ControlPlaneAcceptor {
    val ThreadPrefix = "control-plane"
    val MetricPrefix = "ControlPlane"
}

class ControlPlaneAcceptor(socketServer: SocketServer,
    endPoint: EndPoint,
    config: KafkaConfig,
    nodeId: Int,
    connectionQuotas: ConnectionQuotas,
    time: Time,
    requestChannel: RequestChannel,
    metrics: Metrics,
    credentialProvider: CredentialProvider,
    logContext: LogContext,
    memoryPool: MemoryPool,
    apiVersionManager: ApiVersionManager
)
    extends Acceptor(socketServer,
        endPoint,
        config,
        nodeId,
        connectionQuotas,
        time,
        true,
        requestChannel,
        metrics,
        credentialProvider,
        logContext,
        memoryPool,
        apiVersionManager) {

    override def metricPrefix(): String = ControlPlaneAcceptor.MetricPrefix

    override def threadPrefix(): String = ControlPlaneAcceptor.ThreadPrefix

    def processorOpt(): Option[Processor] = {
        if (processors.isEmpty)
            None
        else
            Some(processors.apply(0))
    }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 * // TODO_MA 马中华 注释： 构造方法
 * // TODO_MA 马中华 注释： run()
 */
private[kafka] abstract class Acceptor(val socketServer: SocketServer,
    val endPoint: EndPoint,
    var config: KafkaConfig,
    nodeId: Int,
    connectionQuotas: ConnectionQuotas,
    time: Time,
    isPrivilegedListener: Boolean,
    requestChannel: RequestChannel,
    metrics: Metrics,
    credentialProvider: CredentialProvider,
    logContext: LogContext,
    memoryPool: MemoryPool,
    apiVersionManager: ApiVersionManager
)
    extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

    def metricPrefix(): String

    def threadPrefix(): String

    private val sendBufferSize = config.socketSendBufferBytes
    private val recvBufferSize = config.socketReceiveBufferBytes
    private val listenBacklogSize = config.socketListenBacklogSize

    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 创建一个 NIO Selector，用来注册 OP_ACCEPT 事件监听客户端的链接请求
     */
    private val nioSelector = NSelector.open()

    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 启动一个 ServerSocketChannel ，绑定 9092 端口
     */
    private[network] val serverChannel = openServerSocket(endPoint.host, endPoint.port, listenBacklogSize)

    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 下游用来处理 OP_READ 事件的 Processor 集合
     */
    private[network] val processors = new ArrayBuffer[Processor]()

    private val processorsStarted = new AtomicBoolean

    // Build the metric name explicitly in order to keep the existing name for compatibility
    private val blockedPercentMeterMetricName = explicitMetricName(
        "kafka.network",
        "Acceptor",
        s"${metricPrefix()}AcceptorBlockedPercent",
        Map(ListenerMetricTag -> endPoint.listenerName.value))

    private val blockedPercentMeter = newMeter(blockedPercentMeterMetricName, "blocked time", TimeUnit.NANOSECONDS)
    private var currentProcessorIndex = 0
    private[network] val throttledSockets = new mutable.PriorityQueue[DelayedCloseSocket]()

    private[network] case class DelayedCloseSocket(socket: SocketChannel, endThrottleTimeMs: Long) extends Ordered[DelayedCloseSocket] {
        override def compare(that: DelayedCloseSocket): Int = endThrottleTimeMs compare that.endThrottleTimeMs
    }

    private[network] def startProcessors(): Unit = synchronized {

        // TODO_MA 马中华 注释： 当 processorsStarted = false，进去了
        if (!processorsStarted.getAndSet(true)) {
            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 启动 Processor
             */
            startProcessors(processors)
        }
    }

    private def startProcessors(processors: Seq[Processor]): Unit = synchronized {

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动 Processor
         */
        processors.foreach { processor =>
            KafkaThread.nonDaemon("", processor).start()
            KafkaThread.nonDaemon(
                s"${threadPrefix()}-kafka-network-thread-$nodeId-${endPoint.listenerName}-${endPoint.securityProtocol}-${processor.id}",
                processor
            ).start()
        }
    }

    private[network] def removeProcessors(removeCount: Int): Unit = synchronized {
        // Shutdown `removeCount` processors. Remove them from the processor list first so that no more
        // connections are assigned. Shutdown the removed processors, closing the selector and its connections.
        // The processors are then removed from `requestChannel` and any pending responses to these processors are dropped.
        val toRemove = processors.takeRight(removeCount)
        processors.remove(processors.size - removeCount, removeCount)
        toRemove.foreach(_.initiateShutdown())
        toRemove.foreach(_.awaitShutdown())
        toRemove.foreach(processor => requestChannel.removeProcessor(processor.id))
    }

    override def initiateShutdown(): Unit = {
        super.initiateShutdown()
        synchronized {
            processors.foreach(_.initiateShutdown())
        }
    }

    override def awaitShutdown(): Unit = {
        super.awaitShutdown()
        synchronized {
            processors.foreach(_.awaitShutdown())
        }
    }

    /**
     * Accept loop that checks for new connection attempts
     * // TODO_MA 马中华 注释： Processor 的核心逻辑，就在这儿
     */
    def run(): Unit = {

        // TODO_MA 马中华 注释： 给 NIO Selector 注册 OP_ACCEPT 事件，等待客户端连接请求过来
        serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： Selector 启动完成
         */
        startupComplete()

        try {

            // TODO_MA 马中华 注释： 只要 Broker 在正常运行，就一直在运行 while() 循环
            while (isRunning) {

                try {
                    /**
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 执行 NIO Selector 的 select() 轮询链接请求事件，如果有可处理的事件，拿出来 accept()
                     *  然后生成 SocketChannel 交给下游组件处理
                     */
                    acceptNewConnections()

                    /**
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 防止过载的限流逻辑
                     */
                    closeThrottledConnections()
                }
                catch {
                    // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
                    // to a select operation on a specific channel or a bad request. We don't want
                    // the broker to stop responding to requests from other clients in these scenarios.
                    case e: ControlThrowable => throw e
                    case e: Throwable => error("Error occurred", e)
                }
            }
        } finally {
            debug("Closing server socket, selector, and any throttled sockets.")

            // TODO_MA 马中华 注释： 关闭 NIO Selector 和 ServerSocketChannel
            CoreUtils.swallow(serverChannel.close(), this, Level.ERROR)
            CoreUtils.swallow(nioSelector.close(), this, Level.ERROR)

            throttledSockets.foreach(throttledSocket => closeSocket(throttledSocket.socket))
            throttledSockets.clear()
            shutdownComplete()
        }
    }

    /**
     * Create a server socket to listen for connections on.
     */
    private def openServerSocket(host: String, port: Int, listenBacklogSize: Int): ServerSocketChannel = {
        val socketAddress =
            if (Utils.isBlank(host))
                new InetSocketAddress(port)
            else
                new InetSocketAddress(host, port)

        // TODO_MA 马中华 注释： 创建 ServerSocketChannel 实例 = NIO 服务端
        val serverChannel = ServerSocketChannel.open()

        serverChannel.configureBlocking(false)
        if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            serverChannel.socket().setReceiveBufferSize(recvBufferSize)

        // TODO_MA 马中华 注释： 绑定 9092 端口
        try {
            serverChannel.socket.bind(socketAddress, listenBacklogSize)
            info(s"Awaiting socket connections on ${socketAddress.getHostString}:${serverChannel.socket.getLocalPort}.")
        } catch {
            case e: SocketException =>
                throw new KafkaException(s"Socket server failed to bind to ${socketAddress.getHostString}:$port: ${e.getMessage}.", e)
        }
        serverChannel
    }

    /**
     * Listen for new connections and assign accepted connections to processors using round-robin.
     */
    private def acceptNewConnections(): Unit = {

        // TODO_MA 马中华 注释： 11111、轮询 OP_ACCEPT 事件
        val ready = nioSelector.select(500)

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 处理 OP_ACCEPT 事件
         */
        if (ready > 0) {

            // TODO_MA 马中华 注释： 拿出来注册 OP_ACCEPT 事件的 SocketChannel 对应的 SelectionKey
            // TODO_MA 马中华 注释： val keys: Set<SeletionKey>
            val keys = nioSelector.selectedKeys()

            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 遍历处理
             */
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
                try {

                    // TODO_MA 马中华 注释： 迭代出来 SelectionKey
                    val key = iter.next
                    iter.remove()

                    /**
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 如果是链接请求，也就是 OP_ACCEPT 事件响应
                     */
                    if (key.isAcceptable) {

                        /**
                         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                         *  注释： 完成链接建立
                         */
                        accept(key).foreach { socketChannel =>
                            // Assign the channel to the next processor (using round-robin) to which the
                            // channel can be added without blocking. If newConnections queue is full on
                            // all processors, block until the last one is able to accept a connection.
                            var retriesLeft = synchronized(processors.length)
                            var processor: Processor = null

                            /**
                             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                             *  注释： 尝试绑定 SocketChannel 到 Processor 上
                             */
                            do {
                                retriesLeft -= 1

                                /**
                                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                                 *  注释： 轮询机制
                                 */
                                processor = synchronized {
                                    // adjust the index (if necessary) and retrieve the processor atomically for
                                    // correct behaviour in case the number of processors is reduced dynamically
                                    // TODO_MA 马中华 注释： 为了保证 currentProcessorIndex < processors.length
                                    currentProcessorIndex = currentProcessorIndex % processors.length
                                    processors(currentProcessorIndex)
                                }
                                // TODO_MA 马中华 注释：  +1
                                currentProcessorIndex += 1

                                /**
                                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                                 *  注释： 然后按照 轮询机制 绑定 SocketChannel 到 Processor 上
                                 */
                            } while (!assignNewConnection(socketChannel, processor, retriesLeft == 0))
                        }
                    } else
                        throw new IllegalStateException("Unrecognized key state for acceptor thread.")
                } catch {
                    case e: Throwable => error("Error while accepting connection", e)
                }
            }
        }
    }

    /**
     * Accept a new connection
     */
    private def accept(key: SelectionKey): Option[SocketChannel] = {
        val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]

        // TODO_MA 马中华 注释： 完成链接建立的实际操作
        val socketChannel = serverSocketChannel.accept()

        try {
            connectionQuotas.inc(endPoint.listenerName, socketChannel.socket.getInetAddress, blockedPercentMeter)

            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 一些常规配置
             */
            configureAcceptedSocketChannel(socketChannel)
            Some(socketChannel)
        } catch {
            case e: TooManyConnectionsException =>
                info(s"Rejected connection from ${e.ip}, address already has the configured maximum of ${e.count} connections.")
                close(endPoint.listenerName, socketChannel)
                None
            case e: ConnectionThrottledException =>
                val ip = socketChannel.socket.getInetAddress
                debug(s"Delaying closing of connection from $ip for ${e.throttleTimeMs} ms")
                val endThrottleTimeMs = e.startThrottleTimeMs + e.throttleTimeMs
                throttledSockets += DelayedCloseSocket(socketChannel, endThrottleTimeMs)
                None
            case e: IOException =>
                error(s"Encountered an error while configuring the connection, closing it.", e)
                close(endPoint.listenerName, socketChannel)
                None
        }
    }

    protected def configureAcceptedSocketChannel(socketChannel: SocketChannel): Unit = {
        socketChannel.configureBlocking(false)
        socketChannel.socket().setTcpNoDelay(true)
        socketChannel.socket().setKeepAlive(true)
        if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            socketChannel.socket().setSendBufferSize(sendBufferSize)
    }

    /**
     * Close sockets for any connections that have been throttled.
     */
    private def closeThrottledConnections(): Unit = {
        val timeMs = time.milliseconds
        while (throttledSockets.headOption.exists(_.endThrottleTimeMs < timeMs)) {
            val closingSocket = throttledSockets.dequeue()
            debug(s"Closing socket from ip ${closingSocket.socket.getRemoteAddress}")
            closeSocket(closingSocket.socket)
        }
    }

    private def assignNewConnection(socketChannel: SocketChannel, processor: Processor, mayBlock: Boolean): Boolean = {

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 完成 SocketChannel 到 Processor 的绑定
         *  内部实现：
         *  1、newConnections.offer(socketChannel)
         *  2、newConnections.put(socketChannel)
         */
        if (processor.accept(socketChannel, mayBlock, blockedPercentMeter)) {
            debug(s"Accepted connection from ${socketChannel.socket.getRemoteSocketAddress} on" +
                s" ${socketChannel.socket.getLocalSocketAddress} and assigned it to processor ${processor.id}," +
                s" sendBufferSize [actual|requested]: [${socketChannel.socket.getSendBufferSize}|$sendBufferSize]" +
                s" recvBufferSize [actual|requested]: [${socketChannel.socket.getReceiveBufferSize}|$recvBufferSize]")
            true
        } else
            false
    }

    /**
     * Wakeup the thread for selection.
     */
    @Override
    def wakeup(): Unit = nioSelector.wakeup()

    def addProcessors(toCreate: Int): Unit = {
        val listenerName = endPoint.listenerName
        val securityProtocol = endPoint.securityProtocol

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 创建 Processor
         */
        val listenerProcessors = new ArrayBuffer[Processor]()
        for (_ <- 0 until toCreate) {
            val processor = newProcessor(socketServer.nextProcessorId(), listenerName, securityProtocol)
            listenerProcessors += processor
            requestChannel.addProcessor(processor)
        }
        processors ++= listenerProcessors

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 启动 Processor
         */
        if (processorsStarted.get)
            startProcessors(listenerProcessors)
    }

    def newProcessor(id: Int, listenerName: ListenerName, securityProtocol: SecurityProtocol): Processor = {
        new Processor(id,
            time,
            config.socketRequestMaxBytes,
            requestChannel,
            connectionQuotas,
            config.connectionsMaxIdleMs,
            config.failedAuthenticationDelayMs,
            listenerName,
            securityProtocol,
            config,
            metrics,
            credentialProvider,
            memoryPool,
            logContext,
            Processor.ConnectionQueueSize,
            isPrivilegedListener,
            apiVersionManager)
    }

}

private[kafka] object Processor {
    val IdlePercentMetricName = "IdlePercent"
    val NetworkProcessorMetricTag = "networkProcessor"
    val ListenerMetricTag = "listener"
    val ConnectionQueueSize = 20
}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 *
 * @param isPrivilegedListener The privileged listener flag is used as one factor to determine whether
 *                             a certain request is forwarded or not. When the control plane is defined,
 *                             the control plane processor would be fellow broker's choice for sending
 *                             forwarding requests; if the control plane is not defined, the processor
 *                             relying on the inter broker listener would be acting as the privileged listener.
 */

/**
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： Processor 自己本身就是一个线程，再启动的时候，会封装成 KafkaThread 启动成守护模式
 *  1、构造器
 *  2、run()
 */
private[kafka] class Processor(val id: Int,
    time: Time,
    maxRequestSize: Int,
    requestChannel: RequestChannel,
    connectionQuotas: ConnectionQuotas,
    connectionsMaxIdleMs: Long,
    failedAuthenticationDelayMs: Int,
    listenerName: ListenerName,
    securityProtocol: SecurityProtocol,
    config: KafkaConfig,
    metrics: Metrics,
    credentialProvider: CredentialProvider,
    memoryPool: MemoryPool,
    logContext: LogContext,
    connectionQueueSize: Int,
    isPrivilegedListener: Boolean,
    apiVersionManager: ApiVersionManager
) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

    private object ConnectionId {
        def fromString(s: String): Option[ConnectionId] = s.split("-") match {
            case Array(local, remote, index) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
                BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
                    ConnectionId(localHost, localPort, remoteHost, remotePort, Integer.parseInt(index))
                }
            }
            case _ => None
        }
    }

    private[network] case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int, index: Int) {
        override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort-$index"
    }

    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 用来存储 Acceptor 创建的客户端链接抽象对象 SocketChannel 的队列
     */
    private val newConnections = new ArrayBlockingQueue[SocketChannel](connectionQueueSize)

    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 响应队列，存储的是 已经发送出去了的 Response，如果最后发送成功了，则会从该集合中移除。
     */
    private val inflightResponses = mutable.Map[String, RequestChannel.Response]()

    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 响应队列，存储的是 刚处理完 Request 之后得到的响应，等待返回给客户端。还没开始写，只是得到了处理结果
     */
    private val responseQueue = new LinkedBlockingDeque[RequestChannel.Response]()

    private[kafka] val metricTags = mutable.LinkedHashMap(
        ListenerMetricTag -> listenerName.value,
        NetworkProcessorMetricTag -> id.toString
    ).asJava

    newGauge(IdlePercentMetricName, () => {
        Option(metrics.metric(metrics.metricName("io-wait-ratio", MetricsGroup, metricTags))).fold(0.0)(m =>
            Math.min(m.metricValue.asInstanceOf[Double], 1.0))
    },
        // for compatibility, only add a networkProcessor tag to the Yammer Metrics alias (the equivalent Selector metric
        // also includes the listener name)
        Map(NetworkProcessorMetricTag -> id.toString)
    )

    val expiredConnectionsKilledCount = new CumulativeSum()
    private val expiredConnectionsKilledCountMetricName = metrics.metricName("expired-connections-killed-count", MetricsGroup, metricTags)
    metrics.addMetric(expiredConnectionsKilledCountMetricName, expiredConnectionsKilledCount)

    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 创建一个 Kafka 封装了 NIO Selector 的 KSelector
     */
    private[network] val selector = createSelector(
        ChannelBuilders.serverChannelBuilder(
            listenerName,
            listenerName == config.interBrokerListenerName,
            securityProtocol,
            config,
            credentialProvider.credentialCache,
            credentialProvider.tokenCache,
            time,
            logContext,
            () => apiVersionManager.apiVersionResponse(throttleTimeMs = 0)
        )
    )

    // Visible to override for testing
    protected[network] def createSelector(channelBuilder: ChannelBuilder): KSelector = {
        channelBuilder match {
            case reconfigurable: Reconfigurable => config.addReconfigurable(reconfigurable)
            case _ =>
        }
        new KSelector(
            maxRequestSize,
            connectionsMaxIdleMs,
            failedAuthenticationDelayMs,
            metrics,
            time,
            "socket-server",
            metricTags,
            false,
            true,
            channelBuilder,
            memoryPool,
            logContext)
    }

    // Connection ids have the format `localAddr:localPort-remoteAddr:remotePort-index`. The index is a
    // non-negative incrementing value that ensures that even if remotePort is reused after a connection is
    // closed, connection ids are not reused while requests from the closed connection are being processed.
    private var nextConnectionIndex = 0

    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 核心业务逻辑在此
     *  总共有 7 个操作，但是只有 5 个重点： 就是前 5 步
     *  1、第一件事： 给 newConnections 中新建立连接注册 OP_READ 事件
     *  2、第二件事： 完成对 responseQueue 队列中的 Response 的处理， 注册 OP_WRITE 事件
     *  3、第三件事： 通过 poll() 方法轮询处理 OP_READ 事件 和 OP_WRITE 事件
     *      0、执行 selector.select() 轮询
     *      1、当处理 OP_READ 事件的时候，会把接收到的数据，放在一个集合中：completedReceives
     *      2、当处理 OP_WRITE 事件的时候，会把要返回的数据，放在一个集合中： completedSends
     *  4、第四件事： 处理 completedReceives，实际上把 每一条数据构建成 Request 然后丢给 RequestChannel
     *     RequestChannel 通过一个队列来维护
     *  5、第五件事： 处理 completedSends 集合：从 inflightResponses 集合中删掉已经发送成功的 Response，同时执行回调
     */
    override def run(): Unit = {

        // TODO_MA 马中华 注释： 完成 Processor 启动
        startupComplete()

        try {

            // TODO_MA 马中华 注释： 里面做了六件事，其中，前面 4 件事尤其重要！
            while (isRunning) {
                try {
                    // setup any new connections that have been queued up
                    /**
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 第一件事： 给 newConnections 中新建立连接注册 OP_READ 事件
                     */
                    configureNewConnections()

                    // register any new responses for writing
                    /**
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 第二件事： 完成对 responseQueue 队列中的 Response 的处理
                     *  1、找到这个 response 对应的 SocketChannel 去登记
                     *  2、注册 OP_WRITE 事件
                     */
                    processNewResponses()

                    /**
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 第三件事： 完成 OP_READ 和 OP_WRITE 事件的处理
                     *  1、执行 select() 去轮询看有没有准备就绪的 OP_READ 和 OP_WRITE 事件
                     *  2、如果有 OP_READ 事件，执行读取，读取到的数据，存放在 completedReceives 集合中
                     *  3、执行 OP_WRITE 事件，执行写出，做了一个登记：存放在 completedSends 集合中
                     */
                    poll()

                    /**
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 第四件事： 处理上一步接收完成的客户端发送过来的数据
                     *  这些刚接收到的请求的数据，都存储在 completedReceives 集合中
                     *  处理是的 OP_READ 事件的处理中，读取到的客户端发送过来的请求数据
                     *  把 completedReceives 里面的每一条数据（其实就是一个请求）封装成 Request 对象
                     *  丢到 RequestChannel 内部的一个队列中
                     *  这个队列： KafkaRequestHandle 线程来消费： 具体做什么： ApisRequstHandler 业务处理
                     */
                    processCompletedReceives()

                    /**
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 第五件事： 处理第三步发送完成的，刚发送完成的，都登记在 completedSends 集合中
                     *  实际上，是从 inflightResponses 集合中删掉已经发送成功的 Response，同时执行回调
                     */
                    processCompletedSends()

                    /**
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 第六件事： 处理链接断开
                     *  已经断开链接的对应的 SocketChannel 会登记在 disconnected 中
                     */
                    processDisconnected()

                    /**
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 第七件事： 关闭其他多余链接
                     *  拿出优先级最低的 SocketChannel 执行关闭
                     */
                    closeExcessConnections()
                } catch {
                    // We catch all the throwables here to prevent the processor thread from exiting. We do this because
                    // letting a processor exit might cause a bigger impact on the broker. This behavior might need to be
                    // reviewed if we see an exception that needs the entire broker to stop. Usually the exceptions thrown would
                    // be either associated with a specific socket channel or a bad request. These exceptions are caught and
                    // processed by the individual methods above which close the failing channel and continue processing other
                    // channels. So this catch block should only ever see ControlThrowables.
                    case e: Throwable => processException("Processor got uncaught exception.", e)
                }
            }
        } finally {
            debug(s"Closing selector - processor $id")
            CoreUtils.swallow(closeAll(), this, Level.ERROR)
            shutdownComplete()
        }
    }

    private[network] def processException(errorMessage: String, throwable: Throwable): Unit = {
        throwable match {
            case e: ControlThrowable => throw e
            case e => error(errorMessage, e)
        }
    }

    private def processChannelException(channelId: String, errorMessage: String, throwable: Throwable): Unit = {
        if (openOrClosingChannel(channelId).isDefined) {
            error(s"Closing socket for $channelId because of error", throwable)
            close(channelId)
        }
        processException(errorMessage, throwable)
    }

    private def processNewResponses(): Unit = {
        var currentResponse: RequestChannel.Response = null

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 处理 responseQueue 中的响应
         */
        while ( {

            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 从队列 responseQueue 中获取 Response
             */
            currentResponse = dequeueResponse();

            // TODO_MA 马中华 注释： 如果队列中有未处理的 Response 则执行处理
            currentResponse != null
        }) {
            val channelId = currentResponse.request.context.connectionId
            try {
                currentResponse match {

                    // TODO_MA 马中华 注释： 什么操作也没有
                    case response: NoOpResponse =>
                        // There is no response to send to the client, we need to read more pipelined requests
                        // that are sitting in the server's socket buffer
                        updateRequestMetrics(response)
                        trace(s"Socket server received empty response to send, registering for read: $response")
                        // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
                        // it will be unmuted immediately. If the channel has been throttled, it will be unmuted only if the
                        // throttling delay has already passed by now.
                        handleChannelMuteEvent(channelId, ChannelMuteEvent.RESPONSE_SENT)
                        tryUnmuteChannel(channelId)

                    /**
                     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                     *  注释： 核心逻辑： 给客户端返回 Response
                     *  这是一个正常的响应
                     *  里面做了两件事：
                     *  1、做了登记
                     *  2、注册 OP_WRITE 事件
                     */
                    case response: SendResponse =>
                        sendResponse(response, response.responseSend)

                    // TODO_MA 马中华 注释： 关闭链接
                    case response: CloseConnectionResponse =>
                        updateRequestMetrics(response)
                        trace("Closing socket connection actively according to the response code.")
                        close(channelId)

                    // TODO_MA 马中华 注释：
                    case _: StartThrottlingResponse =>
                        handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_STARTED)

                    case _: EndThrottlingResponse =>
                        // Try unmuting the channel. The channel will be unmuted only if the response has already been sent out to
                        // the client.
                        handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_ENDED)
                        tryUnmuteChannel(channelId)

                    // TODO_MA 马中华 注释： 不能识别的 response
                    case _ =>
                        throw new IllegalArgumentException(s"Unknown response type: ${currentResponse.getClass}")
                }
            } catch {
                case e: Throwable =>
                    processChannelException(channelId, s"Exception while processing response for $channelId", e)
            }
        }
    }

    // `protected` for test usage
    protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send): Unit = {
        val connectionId = response.request.context.connectionId
        trace(s"Socket server received response to send to $connectionId, registering for write and sending data: $response")
        // `channel` can be None if the connection was closed remotely or if selector closed it for being idle for too long
        if (channel(connectionId).isEmpty) {
            warn(s"Attempting to send response via channel for which there is no open connection, connection id $connectionId")
            response.request.updateRequestMetrics(0L, response)
        }
        // Invoke send for closingChannel as well so that the send is failed and the channel closed properly and
        // removed from the Selector after discarding any pending staged receives.
        // `openOrClosingChannel` can be None if the selector closed the connection because it was idle for too long

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 返回响应 Response 给到客户端
         */
        if (openOrClosingChannel(connectionId).isDefined) {

            // TODO_MA 马中华 注释： 先登记
            selector.send(new NetworkSend(connectionId, responseSend))

            // TODO_MA 马中华 注释： 记录一下
            inflightResponses += (connectionId -> response)
        }
    }

    private def poll(): Unit = {
        val pollTimeout = if (newConnections.isEmpty) 300 else 0

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 获取 OP_READ 和 OP_WRITE 事件来处理
         *  1、KSelector selector， 内部就是封装了 NIO 的 Selector
         *  2、org.apache.kafka.common.network.Selector selector， 也是封装了  NIO 的 Selector
         *  3、nio.Selector selector
         *  KSelector 是 org.apache.kafka.common.network.Selector 的子类
         */
        try selector.poll(pollTimeout)

        catch {
            case e@(_: IllegalStateException | _: IOException) =>
                // The exception is not re-thrown and any completed sends/receives/connections/disconnections
                // from this poll will be processed.
                error(s"Processor $id poll failed", e)
        }
    }

    protected def parseRequestHeader(buffer: ByteBuffer): RequestHeader = {
        val header = RequestHeader.parse(buffer)
        if (apiVersionManager.isApiEnabled(header.apiKey)) {
            header
        } else {
            throw new InvalidRequestException(s"Received request api key ${header.apiKey} which is not enabled")
        }
    }

    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 处理接收到的请求数据
     */
    private def processCompletedReceives(): Unit = {

        // TODO_MA 马中华 注释： 遍历接收到的每一条数据
        // TODO_MA 马中华 注释： receive 就是我们从 客户端读取到的一个请求的数据
        selector.completedReceives.forEach { receive =>
            try {

                // TODO_MA 马中华 注释： 模式匹配， Option
                openOrClosingChannel(receive.source) match {
                    case Some(channel) =>
                        val header = parseRequestHeader(receive.payload)
                        if (header.apiKey == ApiKeys.SASL_HANDSHAKE && channel.maybeBeginServerReauthentication(receive,
                            () => time.nanoseconds()))
                            trace(s"Begin re-authentication: $channel")
                        else {
                            val nowNanos = time.nanoseconds()
                            if (channel.serverAuthenticationSessionExpired(nowNanos)) {
                                // be sure to decrease connection count and drop any in-flight responses
                                debug(s"Disconnecting expired channel: $channel : $header")
                                close(channel.id)
                                expiredConnectionsKilledCount.record(null, 1, 0)
                            } else {

                                // TODO_MA 马中华 注释： 构建请求对象所需要的参数
                                val connectionId = receive.source
                                val context = new RequestContext(header, connectionId, channel.socketAddress,
                                    channel.principal, listenerName, securityProtocol,
                                    channel.channelMetadataRegistry.clientInformation, isPrivilegedListener, channel.principalSerde)

                                // TODO_MA 马中华 注释： 构建请求对象
                                val req = new RequestChannel.Request(processor = id, context = context,
                                    startTimeNanos = nowNanos, memoryPool, receive.payload, requestChannel.metrics, None)

                                // KIP-511: ApiVersionsRequest is intercepted here to catch the client software name
                                // and version. It is done here to avoid wiring things up to the api layer.
                                if (header.apiKey == ApiKeys.API_VERSIONS) {
                                    val apiVersionsRequest = req.body[ApiVersionsRequest]
                                    if (apiVersionsRequest.isValid) {
                                        channel.channelMetadataRegistry.registerClientInformation(new ClientInformation(
                                            apiVersionsRequest.data.clientSoftwareName,
                                            apiVersionsRequest.data.clientSoftwareVersion))
                                    }
                                }

                                /**
                                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                                 *  注释： 将请求注册到 RequestChannel 中
                                 *  RequestChannel 内部维护了一个 ArrayBlockingQueue[BaseRequest] requestQueue 队列
                                 */
                                requestChannel.sendRequest(req)

                                // TODO_MA 马中华 注释： 移除该 SocketChannel 上的 OP_READ 注册事件
                                selector.mute(connectionId)
                                handleChannelMuteEvent(connectionId, ChannelMuteEvent.REQUEST_RECEIVED)
                            }
                        }
                    case None =>
                        // This should never happen since completed receives are processed immediately after `poll()`
                        throw new IllegalStateException(s"Channel ${receive.source} removed from selector before processing completed receive")
                }
            } catch {
                // note that even though we got an exception, we can assume that receive.source is valid.
                // Issues with constructing a valid receive object were handled earlier
                case e: Throwable =>
                    processChannelException(receive.source, s"Exception while processing request from ${receive.source}", e)
            }
        }

        // TODO_MA 马中华 注释： 处理完，清空
        selector.clearCompletedReceives()
    }

    private def processCompletedSends(): Unit = {

        // TODO_MA 马中华 注释： 遍历要发送的
        // TODO_MA 马中华 注释： send 就是返回给给客户端的一个响应
        selector.completedSends.forEach { send =>
            try {

                // TODO_MA 马中华 注释： 如果一个 Response 完成发送，则从 inflightResponses 移除
                val response = inflightResponses.remove(send.destinationId).getOrElse {
                    throw new IllegalStateException(s"Send for ${send.destinationId} completed, but not in `inflightResponses`")
                }
                updateRequestMetrics(response)

                // Invoke send completion callback
                // TODO_MA 马中华 注释： 回调处理
                response.onComplete.foreach(onComplete => onComplete(send))

                // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
                // it will be unmuted immediately. If the channel has been throttled, it will unmuted only if the throttling
                // delay has already passed by now.
                // TODO_MA 马中华 注释： 取消 SocketChannel 的静音，表示可以继续注册 OP_READ 或者 OP_WRITE 事件了
                handleChannelMuteEvent(send.destinationId, ChannelMuteEvent.RESPONSE_SENT)
                tryUnmuteChannel(send.destinationId)
            } catch {
                case e: Throwable => processChannelException(send.destinationId,
                    s"Exception while processing completed send to ${send.destinationId}", e)
            }
        }

        // TODO_MA 马中华 注释： 清空
        selector.clearCompletedSends()
    }

    private def updateRequestMetrics(response: RequestChannel.Response): Unit = {
        val request = response.request
        val networkThreadTimeNanos = openOrClosingChannel(request.context.connectionId).fold(0L)(_.getAndResetNetworkThreadTimeNanos())
        request.updateRequestMetrics(networkThreadTimeNanos, response)
    }

    private def processDisconnected(): Unit = {
        selector.disconnected.keySet.forEach { connectionId =>
            try {
                val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
                    throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
                }.remoteHost
                inflightResponses.remove(connectionId).foreach(updateRequestMetrics)
                // the channel has been closed by the selector but the quotas still need to be updated
                connectionQuotas.dec(listenerName, InetAddress.getByName(remoteHost))
            } catch {
                case e: Throwable => processException(s"Exception while processing disconnection of $connectionId", e)
            }
        }
    }

    private def closeExcessConnections(): Unit = {
        if (connectionQuotas.maxConnectionsExceeded(listenerName)) {
            // TODO_MA 马中华 注释： 拿出优先级最低的 SocketChannel 执行关闭
            val channel = selector.lowestPriorityChannel()
            if (channel != null)
                close(channel.id)
        }
    }

    /**
     * Close the connection identified by `connectionId` and decrement the connection count.
     * The channel will be immediately removed from the selector's `channels` or `closingChannels`
     * and no further disconnect notifications will be sent for this channel by the selector.
     * If responses are pending for the channel, they are dropped and metrics is updated.
     * If the channel has already been removed from selector, no action is taken.
     */
    private def close(connectionId: String): Unit = {
        openOrClosingChannel(connectionId).foreach { channel =>
            debug(s"Closing selector connection $connectionId")
            val address = channel.socketAddress
            if (address != null)
                connectionQuotas.dec(listenerName, address)
            selector.close(connectionId)

            inflightResponses.remove(connectionId).foreach(response => updateRequestMetrics(response))
        }
    }

    /**
     * Queue up a new connection for reading
     */
    def accept(socketChannel: SocketChannel,
        mayBlock: Boolean,
        acceptorIdlePercentMeter: com.yammer.metrics.core.Meter
    ): Boolean = {
        val accepted = {
            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释：
             */
            if (newConnections.offer(socketChannel))
                true
            else if (mayBlock) {
                val startNs = time.nanoseconds
                newConnections.put(socketChannel)
                acceptorIdlePercentMeter.mark(time.nanoseconds() - startNs)
                true
            } else
                false
        }

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释：
         */
        if (accepted)
            wakeup()
        accepted
    }

    /**
     * Register any new connections that have been queued up. The number of connections processed
     * in each iteration is limited to ensure that traffic and connection close notifications of
     * existing channels are handled promptly.
     */
    private def configureNewConnections(): Unit = {
        var connectionsProcessed = 0

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 循环处理
         */
        while (connectionsProcessed < connectionQueueSize && !newConnections.isEmpty) {

            // TODO_MA 马中华 注释： 从完成链接的 newConnections 集合中取出一个 SocketChannel
            val channel = newConnections.poll()
            try {
                debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 给 SocketChannel 注册 OP_READ 事件
                 */
                selector.register(connectionId(channel.socket), channel)

                // TODO_MA 马中华 注释： 计数 +1
                connectionsProcessed += 1
            } catch {
                // We explicitly catch all exceptions and close the socket to avoid a socket leak.
                case e: Throwable =>
                    val remoteAddress = channel.socket.getRemoteSocketAddress
                    // need to close the channel here to avoid a socket leak.
                    close(listenerName, channel)
                    processException(s"Processor $id closed connection from $remoteAddress", e)
            }
        }
    }

    /**
     * Close the selector and all open connections
     */
    private def closeAll(): Unit = {
        while (!newConnections.isEmpty) {
            newConnections.poll().close()
        }
        selector.channels.forEach { channel =>
            close(channel.id)
        }
        selector.close()
        removeMetric(IdlePercentMetricName, Map(NetworkProcessorMetricTag -> id.toString))
    }

    // 'protected` to allow override for testing
    protected[network] def connectionId(socket: Socket): String = {
        val localHost = socket.getLocalAddress.getHostAddress
        val localPort = socket.getLocalPort
        val remoteHost = socket.getInetAddress.getHostAddress
        val remotePort = socket.getPort
        val connId = ConnectionId(localHost, localPort, remoteHost, remotePort, nextConnectionIndex).toString
        nextConnectionIndex = if (nextConnectionIndex == Int.MaxValue) 0 else nextConnectionIndex + 1
        connId
    }

    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 请求的响应，加入到响应队列
     */
    private[network] def enqueueResponse(response: RequestChannel.Response): Unit = {
        responseQueue.put(response)
        wakeup()
    }

    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 从响应队列中，获取响应，返回给客户端
     */
    private def dequeueResponse(): RequestChannel.Response = {

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 从 responseQueue 取响应
         */
        val response = responseQueue.poll()
        if (response != null)
            response.request.responseDequeueTimeNanos = Time.SYSTEM.nanoseconds
        response
    }

    private[network] def responseQueueSize = responseQueue.size

    // Only for testing
    private[network] def inflightResponseCount: Int = inflightResponses.size

    // Visible for testing
    // Only methods that are safe to call on a disconnected channel should be invoked on 'openOrClosingChannel'.
    private[network] def openOrClosingChannel(connectionId: String): Option[KafkaChannel] =
        Option(selector.channel(connectionId)).orElse(Option(selector.closingChannel(connectionId)))

    // Indicate the specified channel that the specified channel mute-related event has happened so that it can change its
    // mute state.
    private def handleChannelMuteEvent(connectionId: String, event: ChannelMuteEvent): Unit = {
        openOrClosingChannel(connectionId).foreach(c => c.handleChannelMuteEvent(event))
    }

    private def tryUnmuteChannel(connectionId: String): Unit = {
        openOrClosingChannel(connectionId).foreach(c => selector.unmute(c.id))
    }

    /* For test usage */
    private[network] def channel(connectionId: String): Option[KafkaChannel] =
        Option(selector.channel(connectionId))

    /**
     * Wakeup the thread for selection.
     */
    override def wakeup(): Unit = selector.wakeup()

    override def initiateShutdown(): Unit = {
        super.initiateShutdown()
        removeMetric("IdlePercent", Map("networkProcessor" -> id.toString))
        metrics.removeMetric(expiredConnectionsKilledCountMetricName)
    }
}

/**
 * Interface for connection quota configuration. Connection quotas can be configured at the
 * broker, listener or IP level.
 */
sealed trait ConnectionQuotaEntity {
    def sensorName: String

    def metricName: String

    def sensorExpiration: Long

    def metricTags: Map[String, String]
}

object ConnectionQuotas {
    private val InactiveSensorExpirationTimeSeconds = TimeUnit.HOURS.toSeconds(1)
    private val ConnectionRateSensorName = "Connection-Accept-Rate"
    private val ConnectionRateMetricName = "connection-accept-rate"
    private val IpMetricTag = "ip"
    private val ListenerThrottlePrefix = ""
    private val IpThrottlePrefix = "ip-"

    private case class ListenerQuotaEntity(listenerName: String) extends ConnectionQuotaEntity {
        override def sensorName: String = s"$ConnectionRateSensorName-$listenerName"

        override def sensorExpiration: Long = Long.MaxValue

        override def metricName: String = ConnectionRateMetricName

        override def metricTags: Map[String, String] = Map(ListenerMetricTag -> listenerName)
    }

    private case object BrokerQuotaEntity extends ConnectionQuotaEntity {
        override def sensorName: String = ConnectionRateSensorName

        override def sensorExpiration: Long = Long.MaxValue

        override def metricName: String = s"broker-$ConnectionRateMetricName"

        override def metricTags: Map[String, String] = Map.empty
    }

    private case class IpQuotaEntity(ip: InetAddress) extends ConnectionQuotaEntity {
        override def sensorName: String = s"$ConnectionRateSensorName-${ip.getHostAddress}"

        override def sensorExpiration: Long = InactiveSensorExpirationTimeSeconds

        override def metricName: String = ConnectionRateMetricName

        override def metricTags: Map[String, String] = Map(IpMetricTag -> ip.getHostAddress)
    }
}

class ConnectionQuotas(config: KafkaConfig, time: Time, metrics: Metrics) extends Logging with AutoCloseable {

    @volatile private var defaultMaxConnectionsPerIp: Int = config.maxConnectionsPerIp
    @volatile private var maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides.map { case (host, count) => (InetAddress.getByName(host), count) }
    @volatile private var brokerMaxConnections = config.maxConnections
    private val interBrokerListenerName = config.interBrokerListenerName
    private val counts = mutable.Map[InetAddress, Int]()

    // Listener counts and configs are synchronized on `counts`
    private val listenerCounts = mutable.Map[ListenerName, Int]()
    private[network] val maxConnectionsPerListener = mutable.Map[ListenerName, ListenerConnectionQuota]()
    @volatile private var totalCount = 0
    // updates to defaultConnectionRatePerIp or connectionRatePerIp must be synchronized on `counts`
    @volatile private var defaultConnectionRatePerIp = QuotaConfigs.IP_CONNECTION_RATE_DEFAULT.intValue()
    private val connectionRatePerIp = new ConcurrentHashMap[InetAddress, Int]()
    // sensor that tracks broker-wide connection creation rate and limit (quota)
    private val brokerConnectionRateSensor = getOrCreateConnectionRateQuotaSensor(config.maxConnectionCreationRate, BrokerQuotaEntity)
    private val maxThrottleTimeMs = TimeUnit.SECONDS.toMillis(config.quotaWindowSizeSeconds.toLong)

    def inc(listenerName: ListenerName, address: InetAddress, acceptorBlockedPercentMeter: com.yammer.metrics.core.Meter): Unit = {
        counts.synchronized {
            waitForConnectionSlot(listenerName, acceptorBlockedPercentMeter)

            recordIpConnectionMaybeThrottle(listenerName, address)
            val count = counts.getOrElseUpdate(address, 0)
            counts.put(address, count + 1)
            totalCount += 1
            if (listenerCounts.contains(listenerName)) {
                listenerCounts.put(listenerName, listenerCounts(listenerName) + 1)
            }
            val max = maxConnectionsPerIpOverrides.getOrElse(address, defaultMaxConnectionsPerIp)
            if (count >= max)
                throw new TooManyConnectionsException(address, max)
        }
    }

    private[network] def updateMaxConnectionsPerIp(maxConnectionsPerIp: Int): Unit = {
        defaultMaxConnectionsPerIp = maxConnectionsPerIp
    }

    private[network] def updateMaxConnectionsPerIpOverride(overrideQuotas: Map[String, Int]): Unit = {
        maxConnectionsPerIpOverrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
    }

    private[network] def updateBrokerMaxConnections(maxConnections: Int): Unit = {
        counts.synchronized {
            brokerMaxConnections = maxConnections
            counts.notifyAll()
        }
    }

    private[network] def updateBrokerMaxConnectionRate(maxConnectionRate: Int): Unit = {
        // if there is a connection waiting on the rate throttle delay, we will let it wait the original delay even if
        // the rate limit increases, because it is just one connection per listener and the code is simpler that way
        updateConnectionRateQuota(maxConnectionRate, BrokerQuotaEntity)
    }

    /**
     * Update the connection rate quota for a given IP and updates quota configs for updated IPs.
     * If an IP is given, metric config will be updated only for the given IP, otherwise
     * all metric configs will be checked and updated if required.
     *
     * @param ip ip to update or default if None
     * @param maxConnectionRate new connection rate, or resets entity to default if None
     */
    def updateIpConnectionRateQuota(ip: Option[InetAddress], maxConnectionRate: Option[Int]): Unit = synchronized {
        def isIpConnectionRateMetric(metricName: MetricName) = {
            metricName.name == ConnectionRateMetricName &&
                metricName.group == MetricsGroup &&
                metricName.tags.containsKey(IpMetricTag)
        }

        def shouldUpdateQuota(metric: KafkaMetric, quotaLimit: Int) = {
            quotaLimit != metric.config.quota.bound
        }

        ip match {
            case Some(address) =>
                // synchronize on counts to ensure reading an IP connection rate quota and creating a quota config is atomic
                counts.synchronized {
                    maxConnectionRate match {
                        case Some(rate) =>
                            info(s"Updating max connection rate override for $address to $rate")
                            connectionRatePerIp.put(address, rate)
                        case None =>
                            info(s"Removing max connection rate override for $address")
                            connectionRatePerIp.remove(address)
                    }
                }
                updateConnectionRateQuota(connectionRateForIp(address), IpQuotaEntity(address))
            case None =>
                // synchronize on counts to ensure reading an IP connection rate quota and creating a quota config is atomic
                counts.synchronized {
                    defaultConnectionRatePerIp = maxConnectionRate.getOrElse(QuotaConfigs.IP_CONNECTION_RATE_DEFAULT.intValue())
                }
                info(s"Updated default max IP connection rate to $defaultConnectionRatePerIp")
                metrics.metrics.forEach { (metricName, metric) =>
                    if (isIpConnectionRateMetric(metricName)) {
                        val quota = connectionRateForIp(InetAddress.getByName(metricName.tags.get(IpMetricTag)))
                        if (shouldUpdateQuota(metric, quota)) {
                            debug(s"Updating existing connection rate quota config for ${metricName.tags} to $quota")
                            metric.config(rateQuotaMetricConfig(quota))
                        }
                    }
                }
        }
    }

    // Visible for testing
    def connectionRateForIp(ip: InetAddress): Int = {
        connectionRatePerIp.getOrDefault(ip, defaultConnectionRatePerIp)
    }

    private[network] def addListener(config: KafkaConfig, listenerName: ListenerName): Unit = {
        counts.synchronized {
            if (!maxConnectionsPerListener.contains(listenerName)) {
                val newListenerQuota = new ListenerConnectionQuota(counts, listenerName)
                maxConnectionsPerListener.put(listenerName, newListenerQuota)
                listenerCounts.put(listenerName, 0)
                config.addReconfigurable(newListenerQuota)
                newListenerQuota.configure(config.valuesWithPrefixOverride(listenerName.configPrefix))
            }
            counts.notifyAll()
        }
    }

    private[network] def removeListener(config: KafkaConfig, listenerName: ListenerName): Unit = {
        counts.synchronized {
            maxConnectionsPerListener.remove(listenerName).foreach { listenerQuota =>
                listenerCounts.remove(listenerName)
                // once listener is removed from maxConnectionsPerListener, no metrics will be recorded into listener's sensor
                // so it is safe to remove sensor here
                listenerQuota.close()
                counts.notifyAll() // wake up any waiting acceptors to close cleanly
                config.removeReconfigurable(listenerQuota)
            }
        }
    }

    def dec(listenerName: ListenerName, address: InetAddress): Unit = {
        counts.synchronized {
            val count = counts.getOrElse(address,
                throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
            if (count == 1)
                counts.remove(address)
            else
                counts.put(address, count - 1)

            if (totalCount <= 0)
                error(s"Attempted to decrease total connection count for broker with no connections")
            totalCount -= 1

            if (maxConnectionsPerListener.contains(listenerName)) {
                val listenerCount = listenerCounts(listenerName)
                if (listenerCount == 0)
                    error(s"Attempted to decrease connection count for listener $listenerName with no connections")
                else
                    listenerCounts.put(listenerName, listenerCount - 1)
            }
            counts.notifyAll() // wake up any acceptors waiting to process a new connection since listener connection limit was reached
        }
    }

    def get(address: InetAddress): Int = counts.synchronized {
        counts.getOrElse(address, 0)
    }

    private def waitForConnectionSlot(listenerName: ListenerName,
        acceptorBlockedPercentMeter: com.yammer.metrics.core.Meter
    ): Unit = {
        counts.synchronized {
            val startThrottleTimeMs = time.milliseconds
            val throttleTimeMs = math.max(recordConnectionAndGetThrottleTimeMs(listenerName, startThrottleTimeMs), 0)

            if (throttleTimeMs > 0 || !connectionSlotAvailable(listenerName)) {
                val startNs = time.nanoseconds
                val endThrottleTimeMs = startThrottleTimeMs + throttleTimeMs
                var remainingThrottleTimeMs = throttleTimeMs
                do {
                    counts.wait(remainingThrottleTimeMs)
                    remainingThrottleTimeMs = math.max(endThrottleTimeMs - time.milliseconds, 0)
                } while (remainingThrottleTimeMs > 0 || !connectionSlotAvailable(listenerName))
                acceptorBlockedPercentMeter.mark(time.nanoseconds - startNs)
            }
        }
    }

    // This is invoked in every poll iteration and we close one LRU connection in an iteration
    // if necessary
    def maxConnectionsExceeded(listenerName: ListenerName): Boolean = {
        totalCount > brokerMaxConnections && !protectedListener(listenerName)
    }

    private def connectionSlotAvailable(listenerName: ListenerName): Boolean = {
        if (listenerCounts(listenerName) >= maxListenerConnections(listenerName))
            false
        else if (protectedListener(listenerName))
            true
        else
            totalCount < brokerMaxConnections
    }

    private def protectedListener(listenerName: ListenerName): Boolean =
        interBrokerListenerName == listenerName && listenerCounts.size > 1

    private def maxListenerConnections(listenerName: ListenerName): Int =
        maxConnectionsPerListener.get(listenerName).map(_.maxConnections).getOrElse(Int.MaxValue)

    /**
     * Calculates the delay needed to bring the observed connection creation rate to listener-level limit or to broker-wide
     * limit, whichever the longest. The delay is capped to the quota window size defined by QuotaWindowSizeSecondsProp
     *
     * @param listenerName listener for which calculate the delay
     * @param timeMs current time in milliseconds
     * @return delay in milliseconds
     */
    private def recordConnectionAndGetThrottleTimeMs(listenerName: ListenerName, timeMs: Long): Long = {
        def recordAndGetListenerThrottleTime(minThrottleTimeMs: Int): Int = {
            maxConnectionsPerListener
                .get(listenerName)
                .map { listenerQuota =>
                    val listenerThrottleTimeMs = recordAndGetThrottleTimeMs(listenerQuota.connectionRateSensor, timeMs)
                    val throttleTimeMs = math.max(minThrottleTimeMs, listenerThrottleTimeMs)
                    // record throttle time due to hitting connection rate quota
                    if (throttleTimeMs > 0) {
                        listenerQuota.listenerConnectionRateThrottleSensor.record(throttleTimeMs.toDouble, timeMs)
                    }
                    throttleTimeMs
                }
                .getOrElse(0)
        }

        if (protectedListener(listenerName)) {
            recordAndGetListenerThrottleTime(0)
        } else {
            val brokerThrottleTimeMs = recordAndGetThrottleTimeMs(brokerConnectionRateSensor, timeMs)
            recordAndGetListenerThrottleTime(brokerThrottleTimeMs)
        }
    }

    /**
     * Record IP throttle time on the corresponding listener. To avoid over-recording listener/broker connection rate, we
     * also un-record the listener and broker connection if the IP gets throttled.
     *
     * @param listenerName listener to un-record connection
     * @param throttleMs IP throttle time to record for listener
     * @param timeMs current time in milliseconds
     */
    private def updateListenerMetrics(listenerName: ListenerName, throttleMs: Long, timeMs: Long): Unit = {
        if (!protectedListener(listenerName)) {
            brokerConnectionRateSensor.record(-1.0, timeMs, false)
        }
        maxConnectionsPerListener
            .get(listenerName)
            .foreach { listenerQuota =>
                listenerQuota.ipConnectionRateThrottleSensor.record(throttleMs.toDouble, timeMs)
                listenerQuota.connectionRateSensor.record(-1.0, timeMs, false)
            }
    }

    /**
     * Calculates the delay needed to bring the observed connection creation rate to the IP limit.
     * If the connection would cause an IP quota violation, un-record the connection for both IP,
     * listener, and broker connection rate and throw a ConnectionThrottledException. Calls to
     * this function must be performed with the counts lock to ensure that reading the IP
     * connection rate quota and creating the sensor's metric config is atomic.
     *
     * @param listenerName listener to unrecord connection if throttled
     * @param address ip address to record connection
     */
    private def recordIpConnectionMaybeThrottle(listenerName: ListenerName, address: InetAddress): Unit = {
        val connectionRateQuota = connectionRateForIp(address)
        val quotaEnabled = connectionRateQuota != QuotaConfigs.IP_CONNECTION_RATE_DEFAULT
        if (quotaEnabled) {
            val sensor = getOrCreateConnectionRateQuotaSensor(connectionRateQuota, IpQuotaEntity(address))
            val timeMs = time.milliseconds
            val throttleMs = recordAndGetThrottleTimeMs(sensor, timeMs)
            if (throttleMs > 0) {
                trace(s"Throttling $address for $throttleMs ms")
                // unrecord the connection since we won't accept the connection
                sensor.record(-1.0, timeMs, false)
                updateListenerMetrics(listenerName, throttleMs, timeMs)
                throw new ConnectionThrottledException(address, timeMs, throttleMs)
            }
        }
    }

    /**
     * Records a new connection into a given connection acceptance rate sensor 'sensor' and returns throttle time
     * in milliseconds if quota got violated
     * @param sensor sensor to record connection
     * @param timeMs current time in milliseconds
     * @return throttle time in milliseconds if quota got violated, otherwise 0
     */
    private def recordAndGetThrottleTimeMs(sensor: Sensor, timeMs: Long): Int = {
        try {
            sensor.record(1.0, timeMs)
            0
        } catch {
            case e: QuotaViolationException =>
                val throttleTimeMs = QuotaUtils.boundedThrottleTime(e, maxThrottleTimeMs, timeMs).toInt
                debug(s"Quota violated for sensor (${sensor.name}). Delay time: $throttleTimeMs ms")
                throttleTimeMs
        }
    }

    /**
     * Creates sensor for tracking the connection creation rate and corresponding connection rate quota for a given
     * listener or broker-wide, if listener is not provided.
     * @param quotaLimit connection creation rate quota
     * @param connectionQuotaEntity entity to create the sensor for
     */
    private def getOrCreateConnectionRateQuotaSensor(quotaLimit: Int, connectionQuotaEntity: ConnectionQuotaEntity): Sensor = {
        Option(metrics.getSensor(connectionQuotaEntity.sensorName)).getOrElse {
            val sensor = metrics.sensor(
                connectionQuotaEntity.sensorName,
                rateQuotaMetricConfig(quotaLimit),
                connectionQuotaEntity.sensorExpiration
            )
            sensor.add(connectionRateMetricName(connectionQuotaEntity), new Rate, null)
            sensor
        }
    }

    /**
     * Updates quota configuration for a given connection quota entity
     */
    private def updateConnectionRateQuota(quotaLimit: Int, connectionQuotaEntity: ConnectionQuotaEntity): Unit = {
        Option(metrics.metric(connectionRateMetricName(connectionQuotaEntity))).foreach { metric =>
            metric.config(rateQuotaMetricConfig(quotaLimit))
            info(s"Updated ${connectionQuotaEntity.metricName} max connection creation rate to $quotaLimit")
        }
    }

    private def connectionRateMetricName(connectionQuotaEntity: ConnectionQuotaEntity): MetricName = {
        metrics.metricName(
            connectionQuotaEntity.metricName,
            MetricsGroup,
            s"Tracking rate of accepting new connections (per second)",
            connectionQuotaEntity.metricTags.asJava)
    }

    private def rateQuotaMetricConfig(quotaLimit: Int): MetricConfig = {
        new MetricConfig()
            .timeWindow(config.quotaWindowSizeSeconds.toLong, TimeUnit.SECONDS)
            .samples(config.numQuotaSamples)
            .quota(new Quota(quotaLimit, true))
    }

    def close(): Unit = {
        metrics.removeSensor(brokerConnectionRateSensor.name)
        maxConnectionsPerListener.values.foreach(_.close())
    }

    class ListenerConnectionQuota(lock: Object, listener: ListenerName) extends ListenerReconfigurable with AutoCloseable {
        @volatile private var _maxConnections = Int.MaxValue
        private[network] val connectionRateSensor = getOrCreateConnectionRateQuotaSensor(Int.MaxValue, ListenerQuotaEntity(listener.value))
        private[network] val listenerConnectionRateThrottleSensor = createConnectionRateThrottleSensor(ListenerThrottlePrefix)
        private[network] val ipConnectionRateThrottleSensor = createConnectionRateThrottleSensor(IpThrottlePrefix)

        def maxConnections: Int = _maxConnections

        override def listenerName(): ListenerName = listener

        override def configure(configs: util.Map[String, _]): Unit = {
            _maxConnections = maxConnections(configs)
            updateConnectionRateQuota(maxConnectionCreationRate(configs), ListenerQuotaEntity(listener.value))
        }

        override def reconfigurableConfigs(): util.Set[String] = {
            SocketServer.ListenerReconfigurableConfigs.asJava
        }

        override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
            val value = maxConnections(configs)
            if (value <= 0)
                throw new ConfigException(s"Invalid ${KafkaConfig.MaxConnectionsProp} $value")

            val rate = maxConnectionCreationRate(configs)
            if (rate <= 0)
                throw new ConfigException(s"Invalid ${KafkaConfig.MaxConnectionCreationRateProp} $rate")
        }

        override def reconfigure(configs: util.Map[String, _]): Unit = {
            lock.synchronized {
                _maxConnections = maxConnections(configs)
                updateConnectionRateQuota(maxConnectionCreationRate(configs), ListenerQuotaEntity(listener.value))
                lock.notifyAll()
            }
        }

        def close(): Unit = {
            metrics.removeSensor(connectionRateSensor.name)
            metrics.removeSensor(listenerConnectionRateThrottleSensor.name)
            metrics.removeSensor(ipConnectionRateThrottleSensor.name)
        }

        private def maxConnections(configs: util.Map[String, _]): Int = {
            Option(configs.get(KafkaConfig.MaxConnectionsProp)).map(_.toString.toInt).getOrElse(Int.MaxValue)
        }

        private def maxConnectionCreationRate(configs: util.Map[String, _]): Int = {
            Option(configs.get(KafkaConfig.MaxConnectionCreationRateProp)).map(_.toString.toInt).getOrElse(Int.MaxValue)
        }

        /**
         * Creates sensor for tracking the average throttle time on this listener due to hitting broker/listener connection
         * rate or IP connection rate quota. The average is out of all throttle times > 0, which is consistent with the
         * bandwidth and request quota throttle time metrics.
         */
        private def createConnectionRateThrottleSensor(throttlePrefix: String): Sensor = {
            val sensor = metrics.sensor(s"${throttlePrefix}ConnectionRateThrottleTime-${listener.value}")
            val metricName = metrics.metricName(s"${throttlePrefix}connection-accept-throttle-time",
                MetricsGroup,
                "Tracking average throttle-time, out of non-zero throttle times, per listener",
                Map(ListenerMetricTag -> listener.value).asJava)
            sensor.add(metricName, new Avg)
            sensor
        }
    }
}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException(s"Too many connections from $ip (maximum = $count)")

class ConnectionThrottledException(val ip: InetAddress, val startThrottleTimeMs: Long, val throttleTimeMs: Long)
    extends KafkaException(s"$ip throttled for $throttleTimeMs")
