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

package kafka.server

import java.io.{File, IOException}
import java.net.{InetAddress, SocketTimeoutException}
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import kafka.api.{KAFKA_0_9_0, KAFKA_2_2_IV0, KAFKA_2_4_IV1}
import kafka.cluster.{Broker, EndPoint}
import kafka.common.{GenerateBrokerIdException, InconsistentBrokerIdException, InconsistentClusterIdException}
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.{ProducerIdManager, TransactionCoordinator}
import kafka.log.LogManager
import kafka.metrics.{KafkaMetricsReporter, KafkaYammerMetrics}
import kafka.network.{ControlPlaneAcceptor, DataPlaneAcceptor, RequestChannel, SocketServer}
import kafka.security.CredentialProvider
import kafka.server.metadata.{ZkConfigRepository, ZkMetadataCache}
import kafka.utils._
import kafka.zk.{AdminZkClient, BrokerInfo, KafkaZkClient}
import org.apache.kafka.clients.{ApiVersions, ManualMetadataUpdater, NetworkClient, NetworkClientUtils}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.ControlledShutdownRequestData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ControlledShutdownRequest, ControlledShutdownResponse}
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.security.{JaasContext, JaasUtils}
import org.apache.kafka.common.utils.{AppInfoParser, LogContext, Time, Utils}
import org.apache.kafka.common.{Endpoint, Node}
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.zookeeper.client.ZKClientConfig

import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

object KafkaServer {

    def zkClientConfigFromKafkaConfig(config: KafkaConfig, forceZkSslClientEnable: Boolean = false): ZKClientConfig = {
        val clientConfig = new ZKClientConfig
        if (config.zkSslClientEnable || forceZkSslClientEnable) {
            KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslClientEnableProp, "true")
            config.zkClientCnxnSocketClassName.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkClientCnxnSocketProp, _))
            config.zkSslKeyStoreLocation.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStoreLocationProp, _))
            config.zkSslKeyStorePassword.foreach(x => KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStorePasswordProp, x.value))
            config.zkSslKeyStoreType.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStoreTypeProp, _))
            config.zkSslTrustStoreLocation.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStoreLocationProp, _))
            config.zkSslTrustStorePassword.foreach(x => KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStorePasswordProp, x.value))
            config.zkSslTrustStoreType.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStoreTypeProp, _))
            KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslProtocolProp, config.ZkSslProtocol)
            config.ZkSslEnabledProtocols.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslEnabledProtocolsProp, _))
            config.ZkSslCipherSuites.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslCipherSuitesProp, _))
            KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp, config.ZkSslEndpointIdentificationAlgorithm)
            KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslCrlEnableProp, config.ZkSslCrlEnable.toString)
            KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslOcspEnableProp, config.ZkSslOcspEnable.toString)
        }
        // The zk sasl is enabled by default so it can produce false error when broker does not intend to use SASL.
        if (!JaasUtils.isZkSaslEnabled) clientConfig.setProperty(JaasUtils.ZK_SASL_CLIENT, "false")
        clientConfig
    }

    val MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS: Long = 120000
}

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 * // TODO_MA 马中华 注释： Scala 的构造方法，就是  类的声明中携带的 (主构造器)， 辅助构造器，就是 方法名叫做 this 的方法
 * // TODO_MA 马中华 注释： 构造器中的代码就是： 类的声明中的 {} 中一切能执行的代码
 * // TODO_MA 马中华 注释： 能执行的代码： 变量的定义，代码块，静态代码，方法的调用，等等
 * // TODO_MA 马中华 注释： 不能执行的代码：内部类，方法的定义
 */
class KafkaServer(
    val config: KafkaConfig,
    time: Time = Time.SYSTEM,
    threadNamePrefix: Option[String] = None,
    enableForwarding: Boolean = false
) extends KafkaBroker with Server {

    private val startupComplete = new AtomicBoolean(false)
    private val isShuttingDown = new AtomicBoolean(false)
    private val isStartingUp = new AtomicBoolean(false)

    @volatile private var _brokerState: BrokerState = BrokerState.NOT_RUNNING
    private var shutdownLatch = new CountDownLatch(1)
    private var logContext: LogContext = null

    private val kafkaMetricsReporters: Seq[KafkaMetricsReporter] =
        KafkaMetricsReporter.startReporters(VerifiableProperties(config.originals))

    var kafkaYammerMetrics: KafkaYammerMetrics = null
    var metrics: Metrics = null

    @volatile var dataPlaneRequestProcessor: KafkaApis = null
    var controlPlaneRequestProcessor: KafkaApis = null

    var authorizer: Option[Authorizer] = None
    @volatile var socketServer: SocketServer = null
    var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = null
    var controlPlaneRequestHandlerPool: KafkaRequestHandlerPool = null

    var logDirFailureChannel: LogDirFailureChannel = null
    @volatile private var _logManager: LogManager = null

    @volatile private var _replicaManager: ReplicaManager = null
    var adminManager: ZkAdminManager = null
    var tokenManager: DelegationTokenManager = null

    var dynamicConfigHandlers: Map[String, ConfigHandler] = null
    var dynamicConfigManager: ZkConfigManager = null
    var credentialProvider: CredentialProvider = null
    var tokenCache: DelegationTokenCache = null

    @volatile var groupCoordinator: GroupCoordinator = null

    var transactionCoordinator: TransactionCoordinator = null

    @volatile private var _kafkaController: KafkaController = null

    var forwardingManager: Option[ForwardingManager] = None

    var autoTopicCreationManager: AutoTopicCreationManager = null

    var clientToControllerChannelManager: BrokerToControllerChannelManager = null

    var alterIsrManager: AlterIsrManager = null

    var kafkaScheduler: KafkaScheduler = null

    @volatile var metadataCache: ZkMetadataCache = null
    var quotaManagers: QuotaFactory.QuotaManagers = null

    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释：
     */
    val zkClientConfig: ZKClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(config)

    private var _zkClient: KafkaZkClient = null
    private var configRepository: ZkConfigRepository = null

    val correlationId: AtomicInteger = new AtomicInteger(0)
    val brokerMetaPropsFile = "meta.properties"
    val brokerMetadataCheckpoints = config.logDirs.map { logDir =>
        (logDir, new BrokerMetadataCheckpoint(new File(logDir + File.separator + brokerMetaPropsFile)))
    }.toMap

    private var _clusterId: String = null
    @volatile var _brokerTopicStats: BrokerTopicStats = null

    private var _featureChangeListener: FinalizedFeatureChangeListener = null

    val brokerFeatures: BrokerFeatures = BrokerFeatures.createDefault()
    val featureCache: FinalizedFeatureCache = new FinalizedFeatureCache(brokerFeatures)

    override def brokerState: BrokerState = _brokerState

    def clusterId: String = _clusterId

    // Visible for testing
    // TODO_MA 马中华 注释： 方法的返回值  void  =  Unit
    /*private[kafka] def zkClient(): KafkaZkClient = {
        _zkClient
    }*/
    private[kafka] def zkClient = _zkClient

    override def brokerTopicStats = _brokerTopicStats

    private[kafka] def featureChangeListener = _featureChangeListener

    override def replicaManager: ReplicaManager = _replicaManager

    override def logManager: LogManager = _logManager

    def kafkaController: KafkaController = _kafkaController

    /**
     * Start up API for bringing up a single instance of the Kafka server.
     * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
     */
    override def startup(): Unit = {
        try {
            info("starting")

            if (isShuttingDown.get)
                throw new IllegalStateException("Kafka server is still shutting down, cannot re-start!")

            if (startupComplete.get)
                return

            // TODO_MA 马中华 注释： Flink 当中，关于 Job 的状态，关于 Task 的状态， Spark Job 和 Task 也有状态表示
            // isStartingUp.set(true)
            val canStartup = isStartingUp.compareAndSet(false, true)

            if (canStartup) {

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 01 件事： 开始启动，状态更新为 STARTING
                 */
                _brokerState = BrokerState.STARTING

                /* setup zookeeper */
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 02 件事： 创建 ZK 客户端，并且初始化 Kafka 在 ZooKeeper 上的 znode 布局
                 *  1、KafkaZkClient ==> ZooKeeperClient ===> ZooKeeper
                 *  2、初始化 Kafka 在 zookeeper 上的 znode 布局
                 */
                initZkClient(time)

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 03 件事： 基于 ZooKeeper 实现的一个配置中心
                 *  配置中心 : ZkConfigRepository 主要就提供了一个方法： config(configResource: ConfigResource)
                 *  用来获取 /config/entityType/entityName 这种格式的 znode 节点的值
                 *  ZKConfigManager
                 */
                configRepository = new ZkConfigRepository(new AdminZkClient(zkClient))

                /* initialize features */
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 04 件事： 构造一个用来监听及同步 /future znode 信息的 监听器
                 *  FinalizedFeatureChangeListener 的创建和初始化
                 */
                _featureChangeListener = new FinalizedFeatureChangeListener(featureCache, _zkClient)
                if (config.isFeatureVersioningSupported) {
                    _featureChangeListener.initOrThrow(config.zkConnectionTimeoutMs)
                }

                /* Get or create cluster_id */
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 05 件事： 从 ZooKeeper 上的 /cluster/id 节点获取 cluster ID
                 *  先尝试从 ZooKeeper 获取，如果不存在，则 基于 UUID 生成一个 cluster ID 创建
                 */
                _clusterId = getOrGenerateClusterId(zkClient)
                info(s"Cluster ID = ${clusterId}")

                /* load metadata */
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 06 件事： 加载本地元数据
                 *  BrokerMetadataCheckpoint 提供了一个 read 和 write 两个用来完成 元数据到 meta.properties 的读写
                 *  1、BrokerMetadata： broker 的元数据，partition 的信息，元数据会持久化到 meta.properties 文件中
                 *  2、OfflineDirs
                 */
                val (preloadedBrokerMetadataCheckpoint, initialOfflineDirs) =
                    BrokerMetadataCheckpoint.getBrokerMetadataAndOfflineDirs(config.logDirs, ignoreMissing = true)

                if (preloadedBrokerMetadataCheckpoint.version != 0) {
                    throw new RuntimeException(s"Found unexpected version in loaded `meta.properties`: " +
                        s"$preloadedBrokerMetadataCheckpoint. Zk-based brokers only support version 0 " +
                        "(which is implicit when the `version` field is missing).")
                }

                /* check cluster id */
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 07 件事： 检查 cluster ID，如果集群之前存在 cluster ID，并且跟刚才创建的 cluster ID 不一致，则产生冲突
                 *  处理方式，就是穷的你故意仓，直接报错。
                 */
                if (preloadedBrokerMetadataCheckpoint.clusterId.isDefined && preloadedBrokerMetadataCheckpoint.clusterId.get != clusterId)
                    throw new InconsistentClusterIdException(
                        s"The Cluster ID ${clusterId} doesn't match stored clusterId ${preloadedBrokerMetadataCheckpoint.clusterId} in meta.properties. " +
                            s"The broker is trying to join the wrong cluster. Configured zookeeper.connect may be wrong.")

                /* generate brokerId */
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 08 件事： 从加载的元数据中获取 broker ID，如果没有的话，则创建一个 broker ID 返回
                 *  1、用户的配置
                 *  2、元数据中存在的
                 *  3、开启了自动创建开关的话，就创建一个
                 */
                config.brokerId = getOrGenerateBrokerId(preloadedBrokerMetadataCheckpoint)
                logContext = new LogContext(s"[KafkaServer id=${config.brokerId}] ")
                this.logIdent = logContext.logPrefix

                // initialize dynamic broker configs from ZooKeeper. Any updates made after this will be
                // applied after ZkConfigManager starts.
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 09 件事： DynamicBrokerConfig 初始化，基于 ZooKeeper 的 broker 动态配置中心
                 *  核心znode节点： /config/brokers/brokerid
                 */
                config.dynamicConfig.initialize(Some(zkClient))

                /* start scheduler */
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 10 件事： 创建和启动 KafkaScheduler
                 *  内部的工作线程数量由 background.threads 参数决定
                 *  内部提供 scheduleOnce() 和 schedule() 用来完成一次性调度 和 周期性调度
                 */
                kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
                kafkaScheduler.startup()

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 11 件事： KafkaMetrics 相关
                 */
                /* create and configure metrics */
                kafkaYammerMetrics = KafkaYammerMetrics.INSTANCE
                kafkaYammerMetrics.configure(config.originals)
                metrics = Server.initializeMetrics(config, time, clusterId)
                /* register broker metrics */
                _brokerTopicStats = new BrokerTopicStats

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 12 件事： 初始化 QuotaManager 配额管理器
                 */
                quotaManagers = QuotaFactory.instantiate(config, metrics, time, threadNamePrefix.getOrElse(""))

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 13 件事： 响应/响应 跟 metric 有关的 CLuster Listener
                 */
                KafkaBroker.notifyClusterListeners(clusterId, kafkaMetricsReporters ++ metrics.reporters.asScala)

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 14 件事： 用来处理 失败日志目录 的组件 LogDirFailureChannel 初始化
                 */
                logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)

                /* start log manager */
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 15 件事： 负责日志管理。 可以认为 LogManager 就是 Broker 中负责处理 Log 相关的一个子系统
                 *  包括日志的读写，检索，过期清理等等工作
                 */
                _logManager = LogManager(
                    config,
                    initialOfflineDirs,
                    configRepository,
                    kafkaScheduler,
                    time,
                    brokerTopicStats,
                    logDirFailureChannel,
                    config.usesTopicId)

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 16 件事： Broker 状态更新为 RECOVERY
                 */
                _brokerState = BrokerState.RECOVERY

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 17 件事： 通过 从 /brokers/topics 下获取集群的 topic 来启动 LogManager
                 *  启动 LogManager 的内部，其实就是 加载 log 信息，并且启动一堆后台服务
                 */
                logManager.startup(zkClient.getAllTopicsInCluster())

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 18 件事： 创建和初始化 ZkMetadataCache
                 *  每个 Broker 内部都为一个了 ZkMetadataCache，通过异步的方式来接收来自 Controller 的 UpdateMetadataRequest
                 *  来进行缓存更新，缓存保存的是 Broker 维护的 Partition 的相关信息，比如 Leader 信息。
                 */
                metadataCache = MetadataCache.zkMetadataCache(config.brokerId)

                // Enable delegation token cache for all SCRAM mechanisms to simplify dynamic update.
                // This keeps the cache up-to-date if new SCRAM mechanisms are enabled dynamically.
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 19 件事： 创建 DelegationTokenCache 和 CredentialProvider
                 */
                tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
                credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 20 件事： BrokerToControllerChannelManager 是用来管理 Broker 和 Controller 之间的链接的组件
                 *  BrokerToControllerChannelManagerImpl 内部运行了一个 BrokerToControllerRequestThread
                 *  并且从 Broker 的元数据缓存中去寻找和链接 Controller
                 */
                clientToControllerChannelManager = BrokerToControllerChannelManager(
                    controllerNodeProvider = MetadataCacheControllerNodeProvider(config, metadataCache),
                    time = time,
                    metrics = metrics,
                    config = config,
                    channelName = "forwarding",
                    threadNamePrefix = threadNamePrefix,
                    retryTimeoutMs = config.requestTimeoutMs.longValue)

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 21 件事： BrokerToControllerChannelManagerImpl 启动
                 *  其实就是启动内部的 BrokerToControllerRequestThread 线程
                 */
                clientToControllerChannelManager.start()

                /* start forwarding manager */
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 22 件事： 创建 ForwardingManager，具体实现类是：ForwardingManagerImpl
                 *  内部提供 forwardRequest 方法用来转发 Broker 请求给 Controller，请求抽象 BrokerToControllerQueueItem
                 */
                var autoTopicCreationChannel = Option.empty[BrokerToControllerChannelManager]
                if (enableForwarding) {
                    this.forwardingManager = Some(ForwardingManager(clientToControllerChannelManager))
                    autoTopicCreationChannel = Some(clientToControllerChannelManager)
                }

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 23 件事： 创建 ApiVersionManager， 具体实现是：DefaultApiVersionManager
                 *  API Version 管理器，管理 Broker 内部之间的通信方式
                 */
                val apiVersionManager = ApiVersionManager(
                    ListenerType.ZK_BROKER,
                    config,
                    forwardingManager,
                    brokerFeatures,
                    featureCache
                )

                // Create and start the socket server acceptor threads so that the bound port is known.
                // Delay starting processors until the end of the initialization sequence to ensure
                // that credentials have been loaded before processing authentications.
                //
                // Note that we allow the use of KRaft mode controller APIs when forwarding is enabled
                // so that the Envelope request is exposed. This is only used in testing currently.
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 24 件事： 创建和启动 SocketServer
                 *  在 startup() 方法中会正儿八经的创建 Acceptor 和 Processor
                 */
                socketServer = new SocketServer(config, metrics, time, credentialProvider, apiVersionManager)
                socketServer.startup(startProcessingRequests = false)

                // Start alter partition manager based on the IBP version
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 25 件事： 创建和启动 AlterIsrManager
                 *  如果 Kafka版本 大于 2.7 的话，是可以支持 修改 ISR 的
                 *  那么就创建一个 AlterIsrManager 负责这个事情， 具体实现是 DefaultAlterIsrManager
                 *  如果 Kafka版本 小于 2.7 的话，具体实现就是 ZkIsrManager
                 */
                alterIsrManager = if (config.interBrokerProtocolVersion.isAlterIsrSupported) {
                    AlterIsrManager(
                        config = config,
                        metadataCache = metadataCache,
                        scheduler = kafkaScheduler,
                        time = time,
                        metrics = metrics,
                        threadNamePrefix = threadNamePrefix,
                        brokerEpochSupplier = () => kafkaController.brokerEpoch
                    )
                } else {
                    AlterIsrManager(kafkaScheduler, time, zkClient)
                }
                alterIsrManager.start()

                /* start replica manager */
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 26 件事： 创建和启动 ReplicaManager
                 */
                _replicaManager = createReplicaManager(isShuttingDown)
                replicaManager.startup()

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 27 件事： Broker 上线注册，将自己的信息注册到 ZooKeeper
                 *  Controller 会去监听 ZooKeeper 的相关 znode 节点，所以会立即知道 Broker 上线的事情
                 *  到 ZooKeeper 上创建 /brokers/ids/brokerid znode
                 *  zkClient() = KafkZKClient（ZookeeperClient（ZooKeeper））
                 */
                val brokerInfo = createBrokerInfo
                val brokerEpoch = zkClient.registerBroker(brokerInfo)

                // Now that the broker is successfully registered, checkpoint its metadata
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 28 件事： 完成对每个 logdir 的元数据持久化工作
                 *  将每个 logdir 的元数据信息，持久化到 meta.properties 文件中
                 */
                checkpointBrokerMetadata(ZkMetaProperties(clusterId, config.brokerId))

                /* start token manager */
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 29 件事： 创建和启动 DelegationTokenManager
                 */
                tokenManager = new DelegationTokenManager(config, tokenCache, time, zkClient)
                tokenManager.startup()

                /* start kafka controller */
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 30 件事： 创建和启动 KafkaController
                 *  1、KafkaController 构造器： 初始化了各种小 工作组件，其中有一个叫做： ControllerEventManager 完成事件的维护和调度
                 *  2、KafkaController 启动方法：
                 *      1、给 ZK 注册监听，监听链接建立的情况
                 *      2、提交 Startup 完成 KafkaController 的具体的启动
                 *      3、启动 ControllerEventManager
                 *  -
                 *  最终 kafkaController 的启动，其实就是执行 processStartup() 完成的
                 */
                _kafkaController = new KafkaController(config, zkClient, time, metrics, brokerInfo, brokerEpoch, tokenManager, brokerFeatures, featureCache, threadNamePrefix)
                kafkaController.startup()

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 31 件事： 创建一个 ZkAdminManager 用来操作 ZooKeeper
                 *  针对 Kafka 存储在 ZooKeeper 上的数据，封装了一些专门的方法，可以快速获取这些信息
                 */
                adminManager = new ZkAdminManager(config, metrics, metadataCache, zkClient)

                /* start group coordinator */
                // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 32 件事： GroupCoordinator 负责 消费者组管理 和 offset 管理
                 */
                groupCoordinator = GroupCoordinator(config, replicaManager, Time.SYSTEM, metrics)
                groupCoordinator.startup(() => zkClient.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME).getOrElse(config.offsetsTopicPartitions))

                /* create producer ids manager */
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 33 件事： 初始化一个专门用来给 Proceduer 生成 ID 的组件
                 */
                val producerIdManager = if (config.interBrokerProtocolVersion.isAllocateProducerIdsSupported) {
                    ProducerIdManager.rpc(
                        config.brokerId,
                        brokerEpochSupplier = () => kafkaController.brokerEpoch,
                        clientToControllerChannelManager,
                        config.requestTimeoutMs
                    )
                } else {
                    ProducerIdManager.zk(config.brokerId, zkClient)
                }

                /* start transaction coordinator, with a separate background thread scheduler for transaction expiration and log loading */
                // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 34 件事： 创建和启动事务协调器
                 */
                transactionCoordinator = TransactionCoordinator(config, replicaManager, new KafkaScheduler(threads = 1, threadNamePrefix = "transaction-log-manager-"),
                    () => producerIdManager, metrics, metadataCache, Time.SYSTEM)
                transactionCoordinator.startup(
                    () => zkClient.getTopicPartitionCount(Topic.TRANSACTION_STATE_TOPIC_NAME).getOrElse(config.transactionTopicPartitions))

                /* start auto topic creation manager */
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 35 件事： 创建 AutoTopicCreationManager
                 *  一个用来自动创建 Topic 的工作组价，具体实现是 DefaultAutoTopicCreationManager
                 *  __consumers_offsets
                 */
                this.autoTopicCreationManager = AutoTopicCreationManager(
                    config,
                    metadataCache,
                    threadNamePrefix,
                    autoTopicCreationChannel,
                    Some(adminManager),
                    Some(kafkaController),
                    groupCoordinator,
                    transactionCoordinator
                )

                /* Get the authorizer and initialize it if one is specified.*/
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 36 件事： 认证相关
                 */
                authorizer = config.authorizer
                authorizer.foreach(_.configure(config.originals))
                val authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = authorizer match {
                    case Some(authZ) =>
                        authZ.start(brokerInfo.broker.toServerInfo(clusterId, config)).asScala.map { case (ep, cs) =>
                            ep -> cs.toCompletableFuture
                        }
                    case None =>
                        brokerInfo.broker.endPoints.map { ep =>
                            ep.toJava -> CompletableFuture.completedFuture[Void](null)
                        }.toMap
                }

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 37 件事： 初始化 FetchManager 实例对象
                 *  内部维护了一个 FetchSessionCache 缓存，用来管理 FetchSession
                 */
                val fetchManager = new FetchManager(Time.SYSTEM,
                    new FetchSessionCache(config.maxIncrementalFetchSessionCacheSlots,
                        KafkaServer.MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS))

                /* start processing requests */
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 38 件事： 一个专门用来告诉是支持 zookeeper 集群模式 还是 raft 集群模式的工具类
                 */
                val zkSupport = ZkSupport(adminManager, kafkaController, zkClient, forwardingManager, metadataCache)

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 39 件事： 定义一个用来创建 KafkaApis 业务处理组件的 方法
                 */
                def createKafkaApis(requestChannel: RequestChannel): KafkaApis = new KafkaApis(
                    requestChannel = requestChannel,
                    metadataSupport = zkSupport,
                    replicaManager = replicaManager,
                    groupCoordinator = groupCoordinator,
                    txnCoordinator = transactionCoordinator,
                    autoTopicCreationManager = autoTopicCreationManager,
                    brokerId = config.brokerId,
                    config = config,
                    configRepository = configRepository,
                    metadataCache = metadataCache,
                    metrics = metrics,
                    authorizer = authorizer,
                    quotas = quotaManagers,
                    fetchManager = fetchManager,
                    brokerTopicStats = brokerTopicStats,
                    clusterId = clusterId,
                    time = time,
                    tokenManager = tokenManager,
                    apiVersionManager = apiVersionManager)

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 40 件事： 创建 KafkaApis，这是 Broker 中特别重要的组件了。
                 *  用来完成 客户端发送过来的请求的具体的处理，比如生产者，消费者的 produce 和 fetch 请求
                 */
                dataPlaneRequestProcessor = createKafkaApis(socketServer.dataPlaneRequestChannel)

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 41 件事： 创建 KafkaRequestHandlerPool
                 *  内部初始化了一组 KafkaRequestHandler 线程用来完成具体的 客户端的请求的业务处理
                 *  这个具体的业务处理组件，就是上面创建的 KafkaApis
                 */
                dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.dataPlaneRequestChannel, dataPlaneRequestProcessor, time,
                    config.numIoThreads, s"${DataPlaneAcceptor.MetricPrefix}RequestHandlerAvgIdlePercent", DataPlaneAcceptor.ThreadPrefix)

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 42 件事： 如果 controlPlane 级别的 RequestChannel 存在
                 *  则创建 KafkaApis 和 KafkaRequestHandlerPool 两大核心组件
                 */
                socketServer.controlPlaneRequestChannelOpt.foreach { controlPlaneRequestChannel =>
                    controlPlaneRequestProcessor = createKafkaApis(controlPlaneRequestChannel)
                    controlPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.controlPlaneRequestChannelOpt.get, controlPlaneRequestProcessor, time,
                        1, s"${ControlPlaneAcceptor.MetricPrefix}RequestHandlerAvgIdlePercent", ControlPlaneAcceptor.ThreadPrefix)
                }

                Mx4jLoader.maybeLoad()

                /* Add all reconfigurables for config change notification before starting config handlers */
                config.dynamicConfig.addReconfigurables(this)
                Option(logManager.cleaner).foreach(config.dynamicConfig.addBrokerReconfigurable)

                /* start dynamic config manager */
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 43 件事： 创建动态配置中心中的不同种类型的配置的 相关 Handler 组件
                 */
                dynamicConfigHandlers = Map[String, ConfigHandler](
                    ConfigType.Topic -> new TopicConfigHandler(logManager, config, quotaManagers, Some(kafkaController)),
                    ConfigType.Client -> new ClientIdConfigHandler(quotaManagers),
                    ConfigType.User -> new UserConfigHandler(quotaManagers, credentialProvider),
                    ConfigType.Broker -> new BrokerConfigHandler(config, quotaManagers),
                    ConfigType.Ip -> new IpConfigHandler(socketServer.connectionQuotas)
                )

                // Create the config manager. start listening to notifications
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 44 件事： 初始化和启动 基于 ZK 的配置中心组件
                 *  该组件专门用来维护： ConfigType.all 中的相关的配置信息
                 */
                dynamicConfigManager = new ZkConfigManager(zkClient, dynamicConfigHandlers)
                dynamicConfigManager.startup()

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 45 件事： 再次尝试一下启动 dataplane 和 controllerplane 的 Acceptor 和 Processor
                 *  一般情况下，dataplane 和 controllerplane 的 Acceptor 和 Processor 组件的启动早就完成了，
                 *  完成了之后，会把 startedProcessingRequests 置为 true
                 */
                socketServer.startProcessingRequests(authorizerFutures)

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 46 件事： 更新 Broker 的运行状态为 RUNNING
                 */
                _brokerState = BrokerState.RUNNING

                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释： 第 47 件事： 修改各种标识，标识 Broker 启动成功了
                 *  过去式  正在进行时
                 */
                shutdownLatch = new CountDownLatch(1)
                startupComplete.set(true)
                isStartingUp.set(false)

                AppInfoParser.registerAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics, time.milliseconds())
                info("started")
            }
        }
        catch {
            case e: Throwable =>
                fatal("Fatal error during KafkaServer startup. Prepare to shutdown", e)
                isStartingUp.set(false)
                shutdown()
                throw e
        }
    }

    protected def createReplicaManager(isShuttingDown: AtomicBoolean): ReplicaManager = {
        new ReplicaManager(
            metrics = metrics,
            config = config,
            time = time,
            scheduler = kafkaScheduler,
            logManager = logManager,
            quotaManagers = quotaManagers,
            metadataCache = metadataCache,
            logDirFailureChannel = logDirFailureChannel,
            alterIsrManager = alterIsrManager,
            brokerTopicStats = brokerTopicStats,
            isShuttingDown = isShuttingDown,
            zkClient = Some(zkClient),
            threadNamePrefix = threadNamePrefix)
    }

    private def initZkClient(time: Time): Unit = {
        info(s"Connecting to zookeeper on ${config.zkConnect}")

        val secureAclsEnabled = config.zkEnableSecureAcls
        val isZkSecurityEnabled = JaasUtils.isZkSaslEnabled() || KafkaConfig.zkTlsClientAuthEnabled(zkClientConfig)

        if (secureAclsEnabled && !isZkSecurityEnabled)
            throw new java.lang.SecurityException(s"${KafkaConfig.ZkEnableSecureAclsProp} is true, but ZooKeeper client TLS configuration identifying at least $KafkaConfig.ZkSslClientEnableProp, $KafkaConfig.ZkClientCnxnSocketProp, and $KafkaConfig.ZkSslKeyStoreLocationProp was not present and the " +
                s"verification of the JAAS login file failed ${JaasUtils.zkSecuritySysConfigString}")

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 创建 KafkaZkClient 实例对象
         *  在 Scala 中创建实例：
         *  1、Stduent s = new Stduent(),  只要有构造器就行，就必须要定义 class Student(主构造器)
         *  2、Student s1 = Student() ， 不用定义  class Student(主构造器)， Studnet.apply()
         */
        _zkClient = KafkaZkClient(config.zkConnect, secureAclsEnabled, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
            config.zkMaxInFlightRequests, time, name = "Kafka server", zkClientConfig = zkClientConfig,
            createChrootIfNecessary = true)

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 初始化 Kafka 在 Zookeeper 上 znode 布局
         */
        _zkClient.createTopLevelPaths()
    }

    private def getOrGenerateClusterId(zkClient: KafkaZkClient): String = {

        // TODO_MA 马中华 注释： /cluster/id
        // TODO_MA 马中华 注释： 先尝试从 zk 获取，如果不存在，则 基于 UUID 生成一个 cluster ID 创建
        zkClient.getClusterId.getOrElse(zkClient.createOrGetClusterId(CoreUtils.generateUuidAsBase64()))
    }

    def createBrokerInfo: BrokerInfo = {
        val endPoints = config.effectiveAdvertisedListeners.map(e => s"${e.host}:${e.port}")
        zkClient.getAllBrokersInCluster.filter(_.id != config.brokerId).foreach { broker =>
            val commonEndPoints = broker.endPoints.map(e => s"${e.host}:${e.port}").intersect(endPoints)
            require(commonEndPoints.isEmpty, s"Configured end points ${commonEndPoints.mkString(",")} in" +
                s" advertised listeners are already registered by broker ${broker.id}")
        }

        val listeners = config.effectiveAdvertisedListeners.map { endpoint =>
            if (endpoint.port == 0)
                endpoint.copy(port = socketServer.boundPort(endpoint.listenerName))
            else
                endpoint
        }

        val updatedEndpoints = listeners.map(endpoint =>
            if (Utils.isBlank(endpoint.host))
                endpoint.copy(host = InetAddress.getLocalHost.getCanonicalHostName)
            else
                endpoint
        )

        val jmxPort = System.getProperty("com.sun.management.jmxremote.port", "-1").toInt

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 生成一个 BrokerInfo 注册信息对象
         */
        BrokerInfo(
            Broker(config.brokerId, updatedEndpoints, config.rack, brokerFeatures.supportedFeatures),
            config.interBrokerProtocolVersion,
            jmxPort)
    }

    /**
     * Performs controlled shutdown
     */
    private def controlledShutdown(): Unit = {
        val socketTimeoutMs = config.controllerSocketTimeoutMs

        def doControlledShutdown(retries: Int): Boolean = {
            val metadataUpdater = new ManualMetadataUpdater()
            val networkClient = {
                val channelBuilder = ChannelBuilders.clientChannelBuilder(
                    config.interBrokerSecurityProtocol,
                    JaasContext.Type.SERVER,
                    config,
                    config.interBrokerListenerName,
                    config.saslMechanismInterBrokerProtocol,
                    time,
                    config.saslInterBrokerHandshakeRequestEnable,
                    logContext)
                val selector = new Selector(
                    NetworkReceive.UNLIMITED,
                    config.connectionsMaxIdleMs,
                    metrics,
                    time,
                    "kafka-server-controlled-shutdown",
                    Map.empty.asJava,
                    false,
                    channelBuilder,
                    logContext
                )
                new NetworkClient(
                    selector,
                    metadataUpdater,
                    config.brokerId.toString,
                    1,
                    0,
                    0,
                    Selectable.USE_DEFAULT_BUFFER_SIZE,
                    Selectable.USE_DEFAULT_BUFFER_SIZE,
                    config.requestTimeoutMs,
                    config.connectionSetupTimeoutMs,
                    config.connectionSetupTimeoutMaxMs,
                    time,
                    false,
                    new ApiVersions,
                    logContext)
            }

            var shutdownSucceeded: Boolean = false

            try {

                var remainingRetries = retries
                var prevController: Node = null
                var ioException = false

                while (!shutdownSucceeded && remainingRetries > 0) {
                    remainingRetries = remainingRetries - 1

                    // 1. Find the controller and establish a connection to it.
                    // If the controller id or the broker registration are missing, we sleep and retry (if there are remaining retries)
                    metadataCache.getControllerId match {
                        case Some(controllerId) =>
                            metadataCache.getAliveBrokerNode(controllerId, config.interBrokerListenerName) match {
                                case Some(broker) =>
                                    // if this is the first attempt, if the controller has changed or if an exception was thrown in a previous
                                    // attempt, connect to the most recent controller
                                    if (ioException || broker != prevController) {

                                        ioException = false

                                        if (prevController != null)
                                            networkClient.close(prevController.idString)

                                        prevController = broker
                                        metadataUpdater.setNodes(Seq(prevController).asJava)
                                    }
                                case None =>
                                    info(s"Broker registration for controller $controllerId is not available in the metadata cache")
                            }
                        case None =>
                            info("No controller present in the metadata cache")
                    }

                    // 2. issue a controlled shutdown to the controller
                    if (prevController != null) {
                        try {

                            if (!NetworkClientUtils.awaitReady(networkClient, prevController, time, socketTimeoutMs))
                                throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

                            // send the controlled shutdown request
                            val controlledShutdownApiVersion: Short =
                                if (config.interBrokerProtocolVersion < KAFKA_0_9_0) 0
                                else if (config.interBrokerProtocolVersion < KAFKA_2_2_IV0) 1
                                else if (config.interBrokerProtocolVersion < KAFKA_2_4_IV1) 2
                                else 3

                            val controlledShutdownRequest = new ControlledShutdownRequest.Builder(
                                new ControlledShutdownRequestData()
                                    .setBrokerId(config.brokerId)
                                    .setBrokerEpoch(kafkaController.brokerEpoch),
                                controlledShutdownApiVersion)
                            val request = networkClient.newClientRequest(prevController.idString, controlledShutdownRequest,
                                time.milliseconds(), true)
                            val clientResponse = NetworkClientUtils.sendAndReceive(networkClient, request, time)

                            val shutdownResponse = clientResponse.responseBody.asInstanceOf[ControlledShutdownResponse]
                            if (shutdownResponse.error != Errors.NONE) {
                                info(s"Controlled shutdown request returned after ${clientResponse.requestLatencyMs}ms " +
                                    s"with error ${shutdownResponse.error}")
                            } else if (shutdownResponse.data.remainingPartitions.isEmpty) {
                                shutdownSucceeded = true
                                info("Controlled shutdown request returned successfully " +
                                    s"after ${clientResponse.requestLatencyMs}ms")
                            } else {
                                info(s"Controlled shutdown request returned after ${clientResponse.requestLatencyMs}ms " +
                                    s"with ${shutdownResponse.data.remainingPartitions.size} partitions remaining to move")

                                if (isDebugEnabled) {
                                    debug("Remaining partitions to move during controlled shutdown: " +
                                        s"${shutdownResponse.data.remainingPartitions}")
                                }
                            }
                        }
                        catch {
                            case ioe: IOException =>
                                ioException = true
                                warn("Error during controlled shutdown, possibly because leader movement took longer than the " +
                                    s"configured controller.socket.timeout.ms and/or request.timeout.ms: ${ioe.getMessage}")
                            // ignore and try again
                        }
                    }
                    if (!shutdownSucceeded && remainingRetries > 0) {
                        Thread.sleep(config.controlledShutdownRetryBackoffMs)
                        info(s"Retrying controlled shutdown ($remainingRetries retries remaining)")
                    }
                }
            }
            finally
                networkClient.close()

            shutdownSucceeded
        }

        if (startupComplete.get() && config.controlledShutdownEnable) {
            // We request the controller to do a controlled shutdown. On failure, we backoff for a configured period
            // of time and try again for a configured number of retries. If all the attempt fails, we simply force
            // the shutdown.
            info("Starting controlled shutdown")

            _brokerState = BrokerState.PENDING_CONTROLLED_SHUTDOWN

            val shutdownSucceeded = doControlledShutdown(config.controlledShutdownMaxRetries.intValue)

            if (!shutdownSucceeded)
                warn("Proceeding to do an unclean shutdown as all the controlled shutdown attempts failed")
        }
    }

    /**
     * Shutdown API for shutting down a single instance of the Kafka server.
     * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
     */
    override def shutdown(): Unit = {
        try {
            info("shutting down")

            if (isStartingUp.get)
                throw new IllegalStateException("Kafka server is still starting up, cannot shut down!")

            // To ensure correct behavior under concurrent calls, we need to check `shutdownLatch` first since it gets updated
            // last in the `if` block. If the order is reversed, we could shutdown twice or leave `isShuttingDown` set to
            // `true` at the end of this method.
            if (shutdownLatch.getCount > 0 && isShuttingDown.compareAndSet(false, true)) {
                CoreUtils.swallow(controlledShutdown(), this)
                _brokerState = BrokerState.SHUTTING_DOWN

                if (dynamicConfigManager != null)
                    CoreUtils.swallow(dynamicConfigManager.shutdown(), this)

                // Stop socket server to stop accepting any more connections and requests.
                // Socket server will be shutdown towards the end of the sequence.
                if (socketServer != null)
                    CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
                if (dataPlaneRequestHandlerPool != null)
                    CoreUtils.swallow(dataPlaneRequestHandlerPool.shutdown(), this)
                if (controlPlaneRequestHandlerPool != null)
                    CoreUtils.swallow(controlPlaneRequestHandlerPool.shutdown(), this)

                /**
                 * We must shutdown the scheduler early because otherwise, the scheduler could touch other
                 * resources that might have been shutdown and cause exceptions.
                 * For example, if we didn't shutdown the scheduler first, when LogManager was closing
                 * partitions one by one, the scheduler might concurrently delete old segments due to
                 * retention. However, the old segments could have been closed by the LogManager, which would
                 * cause an IOException and subsequently mark logdir as offline. As a result, the broker would
                 * not flush the remaining partitions or write the clean shutdown marker. Ultimately, the
                 * broker would have to take hours to recover the log during restart.
                 */
                if (kafkaScheduler != null)
                    CoreUtils.swallow(kafkaScheduler.shutdown(), this)

                if (dataPlaneRequestProcessor != null)
                    CoreUtils.swallow(dataPlaneRequestProcessor.close(), this)
                if (controlPlaneRequestProcessor != null)
                    CoreUtils.swallow(controlPlaneRequestProcessor.close(), this)
                CoreUtils.swallow(authorizer.foreach(_.close()), this)
                if (adminManager != null)
                    CoreUtils.swallow(adminManager.shutdown(), this)

                if (transactionCoordinator != null)
                    CoreUtils.swallow(transactionCoordinator.shutdown(), this)
                if (groupCoordinator != null)
                    CoreUtils.swallow(groupCoordinator.shutdown(), this)

                if (tokenManager != null)
                    CoreUtils.swallow(tokenManager.shutdown(), this)

                if (replicaManager != null)
                    CoreUtils.swallow(replicaManager.shutdown(), this)

                if (alterIsrManager != null)
                    CoreUtils.swallow(alterIsrManager.shutdown(), this)

                if (clientToControllerChannelManager != null)
                    CoreUtils.swallow(clientToControllerChannelManager.shutdown(), this)

                if (logManager != null)
                    CoreUtils.swallow(logManager.shutdown(), this)

                if (kafkaController != null)
                    CoreUtils.swallow(kafkaController.shutdown(), this)

                if (featureChangeListener != null)
                    CoreUtils.swallow(featureChangeListener.close(), this)

                if (zkClient != null)
                    CoreUtils.swallow(zkClient.close(), this)

                if (quotaManagers != null)
                    CoreUtils.swallow(quotaManagers.shutdown(), this)

                // Even though socket server is stopped much earlier, controller can generate
                // response for controlled shutdown request. Shutdown server at the end to
                // avoid any failures (e.g. when metrics are recorded)
                if (socketServer != null)
                    CoreUtils.swallow(socketServer.shutdown(), this)
                if (metrics != null)
                    CoreUtils.swallow(metrics.close(), this)
                if (brokerTopicStats != null)
                    CoreUtils.swallow(brokerTopicStats.close(), this)

                // Clear all reconfigurable instances stored in DynamicBrokerConfig
                config.dynamicConfig.clear()

                _brokerState = BrokerState.NOT_RUNNING

                startupComplete.set(false)
                isShuttingDown.set(false)
                CoreUtils.swallow(AppInfoParser.unregisterAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics), this)
                shutdownLatch.countDown()
                info("shut down completed")
            }
        }
        catch {
            case e: Throwable =>
                fatal("Fatal error during KafkaServer shutdown.", e)
                isShuttingDown.set(false)
                throw e
        }
    }

    /**
     * After calling shutdown(), use this API to wait until the shutdown is complete
     */
    override def awaitShutdown(): Unit = shutdownLatch.await()

    def getLogManager: LogManager = logManager

    override def boundPort(listenerName: ListenerName): Int = socketServer.boundPort(listenerName)

    /** Return advertised listeners with the bound port (this may differ from the configured port if the latter is `0`). */
    def advertisedListeners: Seq[EndPoint] = {
        config.effectiveAdvertisedListeners.map { endPoint =>
            endPoint.copy(port = boundPort(endPoint.listenerName))
        }
    }

    /**
     * Checkpoint the BrokerMetadata to all the online log.dirs
     *
     * @param brokerMetadata
     */
    private def checkpointBrokerMetadata(brokerMetadata: ZkMetaProperties) = {

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 将每个 logdir 的元数据信息，持久化到 meta.properties 文件中
         */
        for (logDir <- config.logDirs if logManager.isLogDirOnline(new File(logDir).getAbsolutePath)) {

            // TODO_MA 马中华 注释：
            val checkpoint = brokerMetadataCheckpoints(logDir)

            // TODO_MA 马中华 注释：
            checkpoint.write(brokerMetadata.toProperties)
        }
    }

    /**
     * Generates new brokerId if enabled or reads from meta.properties based on following conditions
     * <ol>
     * <li> config has no broker.id provided and broker id generation is enabled, generates a broker.id based on Zookeeper's sequence
     * <li> config has broker.id and meta.properties contains broker.id if they don't match throws InconsistentBrokerIdException
     * <li> config has broker.id and there is no meta.properties file, creates new meta.properties and stores broker.id
     * <ol>
     *
     * @return The brokerId.
     */
    private def getOrGenerateBrokerId(brokerMetadata: RawMetaProperties): Int = {

        // TODO_MA 马中华 注释： 用户配置的
        val brokerId = config.brokerId

        // TODO_MA 马中华 注释： 如果 broker ID 存在，并且和元数据中的不相等， 报错
        if (brokerId >= 0 && brokerMetadata.brokerId.exists(_ != brokerId)) {
            throw new InconsistentBrokerIdException(
                s"Configured broker.id $brokerId doesn't match stored broker.id ${brokerMetadata.brokerId} in meta.properties. " +
                    s"If you moved your data, make sure your configured broker.id matches. " +
                    s"If you intend to create a new broker, you should remove all data in your data directories (log.dirs).")

            // TODO_MA 马中华 注释： 元数据中，就直接获取并返回
        } else if (brokerMetadata.brokerId.isDefined) {
            brokerMetadata.brokerId.get

            // TODO_MA 马中华 注释： 如果 broker ID 不存在，而且也开启了自动生成开关的话，就创建一个
        } else if (brokerId < 0 && config.brokerIdGenerationEnable) // generate a new brokerId from Zookeeper
            generateBrokerId()
        else
            brokerId
    }

    /**
     * Return a sequence id generated by updating the broker sequence id path in ZK.
     * Users can provide brokerId in the config. To avoid conflicts between ZK generated
     * sequence id and configured brokerId, we increment the generated sequence id by KafkaConfig.MaxReservedBrokerId.
     */
    private def generateBrokerId(): Int = {
        try {
            zkClient.generateBrokerSequenceId() + config.maxReservedBrokerId
        } catch {
            case e: Exception =>
                error("Failed to generate broker.id due to ", e)
                throw new GenerateBrokerIdException("Failed to generate broker.id", e)
        }
    }
}
