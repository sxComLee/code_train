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

package kafka.log

import kafka.api.ApiVersion
import kafka.log.LogConfig.MessageFormatVersion

import java.io._
import java.nio.file.Files
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import kafka.metrics.KafkaMetricsGroup
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.server.metadata.ConfigRepository
import kafka.server._
import kafka.utils._
import org.apache.kafka.common.{KafkaException, TopicPartition, Uuid}
import org.apache.kafka.common.utils.{KafkaThread, Time, Utils}
import org.apache.kafka.common.errors.{InconsistentTopicIdException, KafkaStorageException, LogDirNotFoundException}

import scala.jdk.CollectionConverters._
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}
import kafka.utils.Implicits._

import java.util.Properties
import scala.annotation.nowarn

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 *
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 *
 * A background thread handles log retention by periodically truncating excess log segments.
 */
@threadsafe
class LogManager(logDirs: Seq[File],
                 initialOfflineDirs: Seq[File],
                 configRepository: ConfigRepository,
                 val initialDefaultConfig: LogConfig,
                 val cleanerConfig: CleanerConfig,
                 recoveryThreadsPerDataDir: Int,
                 val flushCheckMs: Long,
                 val flushRecoveryOffsetCheckpointMs: Long,
                 val flushStartOffsetCheckpointMs: Long,
                 val retentionCheckMs: Long,
                 val maxPidExpirationMs: Int,
                 interBrokerProtocolVersion: ApiVersion,
                 scheduler: Scheduler,
                 brokerTopicStats: BrokerTopicStats,
                 logDirFailureChannel: LogDirFailureChannel,
                 time: Time,
                 val keepPartitionMetadataFile: Boolean) extends Logging with KafkaMetricsGroup {

  import LogManager._

  //todo:默认的锁文件，kafka不正常关闭的时候，就会看到这个文件没有清理完，在logdir路径下
  val LockFile = ".lock"
  //todo:初始任务延迟时长
  val InitialTaskDelayMs = 30 * 1000
  //创建或删除 Log 时的锁对象
  private val logCreationOrDeletionLock = new Object
  /** 记录每个 topic 分区对象与 Log 对象之间的映射关系 */
  private val currentLogs = new Pool[TopicPartition, UnifiedLog]()
  // Future logs are put in the directory with "-future" suffix. Future log is created when user wants to move replica
  // from one log directory to another log directory on the same broker. The directory of the future log will be renamed
  // to replace the current log of the partition after the future log catches up with the current log
  private val futureLogs = new Pool[TopicPartition, UnifiedLog]()
  // Each element in the queue contains the log object to be deleted and the time it is scheduled for deletion.
  /** 记录需要被删除的 Log 对象 */
  private val logsToBeDeleted = new LinkedBlockingQueue[(UnifiedLog, Long)]()
  //todo: 检查目录是否合法,并且创建目录
  private val _liveLogDirs: ConcurrentLinkedQueue[File] = createAndValidateLogDirs(logDirs, initialOfflineDirs)
  @volatile private var _currentDefaultConfig = initialDefaultConfig
  @volatile private var numRecoveryThreadsPerDataDir = recoveryThreadsPerDataDir

  // This map contains all partitions whose logs are getting loaded and initialized. If log configuration
  // of these partitions get updated at the same time, the corresponding entry in this map is set to "true",
  // which triggers a config reload after initialization is finished (to get the latest config value).
  // See KAFKA-8813 for more detail on the race condition
  // Visible for testing
  private[log] val partitionsInitializing = new ConcurrentHashMap[TopicPartition, Boolean]().asScala

  def reconfigureDefaultLogConfig(logConfig: LogConfig): Unit = {
    this._currentDefaultConfig = logConfig
  }

  def currentDefaultConfig: LogConfig = _currentDefaultConfig

  def liveLogDirs: Seq[File] = {
    if (_liveLogDirs.size == logDirs.size)
      logDirs
    else
      _liveLogDirs.asScala.toBuffer
  }
  /** 尝试对每个 log 目录在文件系统层面加锁，这里加的是进程锁 */
  private val dirLocks = lockLogDirs(liveLogDirs)

  /**
   * 遍历为每个 log 目录创建一个操作其名下 recovery-point-offset-checkpoint 文件的 OffsetCheckpoint 对象，
   * 并建立映射关系
   */
  @volatile private var recoveryPointCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, RecoveryPointCheckpointFile), logDirFailureChannel))).toMap
  @volatile private var logStartOffsetCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, LogStartOffsetCheckpointFile), logDirFailureChannel))).toMap

  private val preferredLogDirs = new ConcurrentHashMap[TopicPartition, String]()

  private def offlineLogDirs: Iterable[File] = {
    val logDirsSet = mutable.Set[File]() ++= logDirs
    _liveLogDirs.forEach(dir => logDirsSet -= dir)
    logDirsSet
  }

  @volatile private var _cleaner: LogCleaner = _
  private[kafka] def cleaner: LogCleaner = _cleaner

  newGauge("OfflineLogDirectoryCount", () => offlineLogDirs.size)

  for (dir <- logDirs) {
    newGauge("LogDirectoryOffline",
      () => if (_liveLogDirs.contains(dir)) 0 else 1,
      Map("logDirectory" -> dir.getAbsolutePath))
  }

  /**
   * Create and check validity of the given directories that are not in the given offline directories, specifically:
   * <ol>
   * <li> Ensure that there are no duplicates in the directory list
   * <li> Create each directory if it doesn't exist
   * <li> Check that each path is a readable directory
   * </ol>
   */
  private def createAndValidateLogDirs(dirs: Seq[File], initialOfflineDirs: Seq[File]): ConcurrentLinkedQueue[File] = {
    //创建了一个队列
    val liveLogDirs = new ConcurrentLinkedQueue[File]()
    val canonicalPaths = mutable.HashSet.empty[String]
    //循环遍历所有的路径
    for (dir <- dirs) {
      try {
        //初始化就掉线的路径，包含遍历出来的路径，直接抛出异常
        if (initialOfflineDirs.contains(dir))
          throw new IOException(s"Failed to load ${dir.getAbsolutePath} during broker startup")
        //todo:路径不存在，创建路径
        if (!dir.exists) {
          info(s"Log directory ${dir.getAbsolutePath} not found, creating it.")
          val created = dir.mkdirs()
          if (!created)
            throw new IOException(s"Failed to create data directory ${dir.getAbsolutePath}")
          Utils.flushDir(dir.toPath.toAbsolutePath.normalize.getParent)
        }
        //todo:如果是文件或者路径不能读取，直接抛出异常
        if (!dir.isDirectory || !dir.canRead)
          throw new IOException(s"${dir.getAbsolutePath} is not a readable log directory.")

        // getCanonicalPath() throws IOException if a file system query fails or if the path is invalid (e.g. contains
        // the Nul character). Since there's no easy way to distinguish between the two cases, we treat them the same
        // and mark the log directory as offline.
        if (!canonicalPaths.add(dir.getCanonicalPath))
          throw new KafkaException(s"Duplicate log directory found: ${dirs.mkString(", ")}")

        //正常的路径，添加到队列里面去
        liveLogDirs.add(dir)
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Failed to create or validate data directory ${dir.getAbsolutePath}", e)
      }
    }
    if (liveLogDirs.isEmpty) {
      fatal(s"Shutdown broker because none of the specified log dirs from ${dirs.mkString(", ")} can be created or validated")
      Exit.halt(1)
    }

    liveLogDirs
  }

  def resizeRecoveryThreadPool(newSize: Int): Unit = {
    info(s"Resizing recovery thread pool size for each data dir from $numRecoveryThreadsPerDataDir to $newSize")
    numRecoveryThreadsPerDataDir = newSize
  }

  /**
   * The log directory failure handler. It will stop log cleaning in that directory.
   *
   * @param dir        the absolute path of the log directory
   */
  def handleLogDirFailure(dir: String): Unit = {
    warn(s"Stopping serving logs in dir $dir")
    logCreationOrDeletionLock synchronized {
      _liveLogDirs.remove(new File(dir))
      if (_liveLogDirs.isEmpty) {
        fatal(s"Shutdown broker because all log dirs in ${logDirs.mkString(", ")} have failed")
        Exit.halt(1)
      }

      recoveryPointCheckpoints = recoveryPointCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      logStartOffsetCheckpoints = logStartOffsetCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      if (cleaner != null)
        cleaner.handleLogDirFailure(dir)

      def removeOfflineLogs(logs: Pool[TopicPartition, UnifiedLog]): Iterable[TopicPartition] = {
        val offlineTopicPartitions: Iterable[TopicPartition] = logs.collect {
          case (tp, log) if log.parentDir == dir => tp
        }
        offlineTopicPartitions.foreach { topicPartition => {
          val removedLog = removeLogAndMetrics(logs, topicPartition)
          removedLog.foreach {
            log => log.closeHandlers()
          }
        }}

        offlineTopicPartitions
      }

      val offlineCurrentTopicPartitions = removeOfflineLogs(currentLogs)
      val offlineFutureTopicPartitions = removeOfflineLogs(futureLogs)

      warn(s"Logs for partitions ${offlineCurrentTopicPartitions.mkString(",")} are offline and " +
           s"logs for future partitions ${offlineFutureTopicPartitions.mkString(",")} are offline due to failure on log directory $dir")
      dirLocks.filter(_.file.getParent == dir).foreach(dir => CoreUtils.swallow(dir.destroy(), this))
    }
  }

  /**
   * Lock all the given directories
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    dirs.flatMap { dir =>
      try {
        val lock = new FileLock(new File(dir, LockFile))
        if (!lock.tryLock())
          throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParent +
            ". A Kafka instance in another process or thread is using this directory.")
        Some(lock)
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while locking directory $dir", e)
          None
      }
    }
  }

  private def addLogToBeDeleted(log: UnifiedLog): Unit = {
    this.logsToBeDeleted.add((log, time.milliseconds()))
  }

  // Only for testing
  private[log] def hasLogsToBeDeleted: Boolean = !logsToBeDeleted.isEmpty

  private[log] def loadLog(logDir: File,
                           hadCleanShutdown: Boolean,
                           recoveryPoints: Map[TopicPartition, Long],
                           logStartOffsets: Map[TopicPartition, Long],
                           defaultConfig: LogConfig,
                           topicConfigOverrides: Map[String, LogConfig]): UnifiedLog = {
    //todo:获取topic以及partition名称
    val topicPartition = UnifiedLog.parseTopicPartitionName(logDir)
    val config = topicConfigOverrides.getOrElse(topicPartition.topic, defaultConfig)
    //todo：获取日志恢复点
    val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)
    //todo:定义日志起始offset
    val logStartOffset = logStartOffsets.getOrElse(topicPartition, 0L)
    //todo：
    val log = UnifiedLog(
      dir = logDir,
      config = config,
      logStartOffset = logStartOffset,
      recoveryPoint = logRecoveryPoint,
      maxProducerIdExpirationMs = maxPidExpirationMs,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      scheduler = scheduler,
      time = time,
      brokerTopicStats = brokerTopicStats,
      logDirFailureChannel = logDirFailureChannel,
      lastShutdownClean = hadCleanShutdown,
      topicId = None,
      keepPartitionMetadataFile = keepPartitionMetadataFile)

    if (logDir.getName.endsWith(UnifiedLog.DeleteDirSuffix)) {
      //todo:判断如果日志有删除的标识，将日志添加到待删除里面去
      addLogToBeDeleted(log)
    } else {
      val previous = {
        if (log.isFuture)
          //todo：需要移动的日志，都房子啊这个线程池里面来处理
          this.futureLogs.put(topicPartition, log)
        else
        //当前直接添加的日志，都放到这个线程池里面来处理
          this.currentLogs.put(topicPartition, log)
      }
      if (previous != null) {
        if (log.isFuture)
          throw new IllegalStateException(s"Duplicate log directories found: ${log.dir.getAbsolutePath}, ${previous.dir.getAbsolutePath}")
        else
          throw new IllegalStateException(s"Duplicate log directories for $topicPartition are found in both ${log.dir.getAbsolutePath} " +
            s"and ${previous.dir.getAbsolutePath}. It is likely because log directory failure happened while broker was " +
            s"replacing current replica with future replica. Recover broker from this failure by manually deleting one of the two directories " +
            s"for this partition. It is recommended to delete the partition in the log directory that is known to have failed recently.")
      }
    }

    log
  }

  /**
   * Recover and load all logs in the given data directories
   */
  private[log] def loadLogs(defaultConfig: LogConfig, topicConfigOverrides: Map[String, LogConfig]): Unit = {
    info(s"Loading logs from log dirs $liveLogDirs")
    val startMs = time.hiResClockMs()
    //构建了一个线程池
    val threadPools = ArrayBuffer.empty[ExecutorService]

    //用于保存下线的日志文件路径
    val offlineDirs = mutable.Set.empty[(String, IOException)]
    val jobs = ArrayBuffer.empty[Seq[Future[_]]]
    var numTotalLogs = 0
    //循环遍历每一个日志保存路径
    for (dir <- liveLogDirs) {
      val logDirAbsolutePath = dir.getAbsolutePath
      var hadCleanShutdown: Boolean = false
      try {
        //todo: 对每个日志目录创建numRecoveryThreadsPerDataDir(默认为1)个线程组成的线程池，并加入threadPools中
        val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir,
          KafkaThread.nonDaemon(s"log-recovery-$logDirAbsolutePath", _))
        threadPools.append(pool)
        //检查上一次关闭是否是正常关闭
        val cleanShutdownFile = new File(dir, LogLoader.CleanShutdownFile)

        // 检查.kafka_cleanshutdown文件是否存在
        // 存在则表示Kafka正在经历清理性的停机工作，此时跳过从本文件夹恢复日志
        if (cleanShutdownFile.exists) {
          info(s"Skipping recovery for all logs in $logDirAbsolutePath since clean shutdown file was found")
          // Cache the clean shutdown status and use that for rest of log loading workflow. Delete the CleanShutdownFile
          // so that if broker crashes while loading the log, it is considered hard shutdown during the next boot up. KAFKA-10471
          Files.deleteIfExists(cleanShutdownFile.toPath)
          hadCleanShutdown = true
        } else {
          // log recovery itself is being performed by `Log` class during initialization
          info(s"Attempting recovery for all logs in $logDirAbsolutePath since no clean shutdown file was found")
        }
        // 从检查点文件读取Topic对应的恢复点offset信息
        // 文件为recovery-point-offset-checkpoint
        // checkpoint file format:
        // line1 : version
        // line2 : expectedSize
        // nlines: (tp, offset)
        var recoveryPoints = Map[TopicPartition, Long]()
        try {
          //从恢复点读取Topic对应的startoffset信息
          recoveryPoints = this.recoveryPointCheckpoints(dir).read()
        } catch {
          case e: Exception =>
            warn(s"Error occurred while reading recovery-point-offset-checkpoint file of directory " +
              s"$logDirAbsolutePath, resetting the recovery checkpoint to 0", e)
        }

        var logStartOffsets = Map[TopicPartition, Long]()
        try {
          // 从检查点文件读取Topic对应的startoffset信息
          // 文件为log-start-offset-checkpoint
          // checkpoint file format:
          // line1 : version
          // line2 : expectedSize
          // nlines: (tp, startoffset)
          logStartOffsets = this.logStartOffsetCheckpoints(dir).read()
        } catch {
          case e: Exception =>
            warn(s"Error occurred while reading log-start-offset-checkpoint file of directory " +
              s"$logDirAbsolutePath, resetting to the base offset of the first segment", e)
        }

        val logsToLoad = Option(dir.listFiles).getOrElse(Array.empty).filter(logDir =>
          logDir.isDirectory && UnifiedLog.parseTopicPartitionName(logDir).topic != KafkaRaftServer.MetadataTopic)
        val numLogsLoaded = new AtomicInteger(0)
        numTotalLogs += logsToLoad.length

        // 每个日志子目录生成一个线程池的具体job，并提交到线程池中
        // 每个job的主要任务是通过loadLog()方法加载日志
        val jobsForDir = logsToLoad.map { logDir =>
          val runnable: Runnable = () => {
            try {
              debug(s"Loading log $logDir")

              val logLoadStartMs = time.hiResClockMs()
              //todo:  每个日志子目录生成一个job加载log，并且根据读取的recoveryPoints, logStartOffsets进行加载
              val log = loadLog(logDir, hadCleanShutdown, recoveryPoints, logStartOffsets,
                defaultConfig, topicConfigOverrides)
              val logLoadDurationMs = time.hiResClockMs() - logLoadStartMs
              val currentNumLoaded = numLogsLoaded.incrementAndGet()

              info(s"Completed load of $log with ${log.numberOfSegments} segments in ${logLoadDurationMs}ms " +
                s"($currentNumLoaded/${logsToLoad.length} loaded in $logDirAbsolutePath)")
            } catch {
              case e: IOException =>
                offlineDirs.add((logDirAbsolutePath, e))
                error(s"Error while loading log dir $logDirAbsolutePath", e)
            }
          }
          runnable
        }

        //todo:添加到集合当中去  调用pool.submit运行线程
        jobs += jobsForDir.map(pool.submit)
      } catch {
        case e: IOException =>
          offlineDirs.add((logDirAbsolutePath, e))
          error(s"Error while loading log dir $logDirAbsolutePath", e)
      }
    }

    try {
      for (dirJobs <- jobs) {
        dirJobs.foreach(_.get)
      }

      offlineDirs.foreach { case (dir, e) =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir, s"Error while loading log dir $dir", e)
      }
    } catch {
      case e: ExecutionException =>
        error(s"There was an error in one of the threads during logs loading: ${e.getCause}")
        throw e.getCause
    } finally {
      threadPools.foreach(_.shutdown())
    }

    info(s"Loaded $numTotalLogs logs in ${time.hiResClockMs() - startMs}ms.")
  }

  /**
   *  Start the background threads to flush logs and do log cleanup
   */
  def startup(topicNames: Set[String]): Unit = {
    // ensure consistency between default config and overrides
    val defaultConfig = currentDefaultConfig
    startupWithConfigOverrides(defaultConfig, fetchTopicConfigOverrides(defaultConfig, topicNames))
  }

  // visible for testing
  @nowarn("cat=deprecation")
  private[log] def fetchTopicConfigOverrides(defaultConfig: LogConfig, topicNames: Set[String]): Map[String, LogConfig] = {
    val topicConfigOverrides = mutable.Map[String, LogConfig]()
    val defaultProps = defaultConfig.originals()
    topicNames.foreach { topicName =>
      var overrides = configRepository.topicConfig(topicName)
      // save memory by only including configs for topics with overrides
      if (!overrides.isEmpty) {
        Option(overrides.getProperty(LogConfig.MessageFormatVersionProp)).foreach { versionString =>
          val messageFormatVersion = new MessageFormatVersion(versionString, interBrokerProtocolVersion.version)
          if (messageFormatVersion.shouldIgnore) {
            val copy = new Properties()
            copy.putAll(overrides)
            copy.remove(LogConfig.MessageFormatVersionProp)
            overrides = copy

            if (messageFormatVersion.shouldWarn)
              warn(messageFormatVersion.topicWarningMessage(topicName))
          }
        }

        val logConfig = LogConfig.fromProps(defaultProps, overrides)
        topicConfigOverrides(topicName) = logConfig
      }
    }
    topicConfigOverrides
  }

  private def fetchLogConfig(topicName: String): LogConfig = {
    // ensure consistency between default config and overrides
    val defaultConfig = currentDefaultConfig
    fetchTopicConfigOverrides(defaultConfig, Set(topicName)).values.headOption.getOrElse(defaultConfig)
  }

  // visible for testing
  private[log] def startupWithConfigOverrides(defaultConfig: LogConfig, topicConfigOverrides: Map[String, LogConfig]): Unit = {
    //在这个方法里面运行了加载所有的已经写入的日志数据
    loadLogs(defaultConfig, topicConfigOverrides) // this could take a while if shutdown was not clean

    /* Schedule the cleanup task to delete old logs */
    if (scheduler != null) {
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      //todo: 1. 启动 kafka-log-retention 周期性任务，对过期或过大的日志文件执行清理工作
      //遍历所有Log。负责清理未压缩的日志，清除条件【1.日志超过保留时间 2.日志大小超过保留大小】】
      scheduler.schedule("kafka-log-retention",
                         cleanupLogs _, //定时执行的方法
                         delay = InitialTaskDelayMs, //启动之后30s开始定时调度
                         period = retentionCheckMs,  //log.retention.check.interval.ms 默认为5分钟
                         TimeUnit.MILLISECONDS)
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      //todo: 2. 启动 kafka-log-flusher 周期性任务，对日志文件执行刷盘操作,定时把内存中的数据刷到磁盘中
      scheduler.schedule("kafka-log-flusher",
                         flushDirtyLogs _,//定时执行的方法
                         delay = InitialTaskDelayMs,  //启动之后30s开始定时调度
                         period = flushCheckMs, //log.flush.scheduler.interval.ms 默认值为Long.MaxValue
                         TimeUnit.MILLISECONDS)

      //todo: 3. 启动 kafka-recovery-point-checkpoint 周期性任务，更新 recovery-point-offset-checkpoint 文件
      //向路径中写入当前的恢复点，避免在重启时需要重新恢复全部数据
      scheduler.schedule("kafka-recovery-point-checkpoint",
                         checkpointLogRecoveryOffsets _, //定时执行的方法
                         delay = InitialTaskDelayMs, //启动之后30s开始定时调度
                         period = flushRecoveryOffsetCheckpointMs,//log.flush.offset.checkpoint.interval.ms 默认为1分钟
                         TimeUnit.MILLISECONDS)
      //todo: 4. 启动 kafka-log-start-offset-checkpoint 周期性任务，更新 kafka-log-start-offset-checkpoint 文件
      //向日志目录写入当前存储的日志中的start offset。避免读到已经被删除的日志
      scheduler.schedule("kafka-log-start-offset-checkpoint",
                         checkpointLogStartOffsets _,//定时执行的方法
                         delay = InitialTaskDelayMs,//启动之后30s开始定时调度
                         period = flushStartOffsetCheckpointMs,//log.flush.start.offset.checkpoint.interval.ms 默认为1分钟
                         TimeUnit.MILLISECONDS)
      //todo: 5. 启动 kafka-delete-logs 周期性任务，清理已经被标记为删除delete的日志
      scheduler.schedule("kafka-delete-logs", // will be rescheduled after each delete logs with a dynamic period
                         deleteLogs _,//定时执行的方法
                         delay = InitialTaskDelayMs, //启动之后30s开始定时调度
                         unit = TimeUnit.MILLISECONDS)
    }
    //启动日志清理组件
    if (cleanerConfig.enableCleaner) {
      _cleaner = new LogCleaner(cleanerConfig, liveLogDirs, currentLogs, logDirFailureChannel, time = time)
      _cleaner.startup()
    }
  }

  /**
   * Close all the logs
   */
  def shutdown(): Unit = {
    info("Shutting down.")

    removeMetric("OfflineLogDirectoryCount")
    for (dir <- logDirs) {
      removeMetric("LogDirectoryOffline", Map("logDirectory" -> dir.getAbsolutePath))
    }

    val threadPools = ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // stop the cleaner first
    if (cleaner != null) {
      CoreUtils.swallow(cleaner.shutdown(), this)
    }

    val localLogsByDir = logsByDir

    // close logs in each dir
    for (dir <- liveLogDirs) {
      debug(s"Flushing and closing logs at $dir")

      val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir,
        KafkaThread.nonDaemon(s"log-closing-${dir.getAbsolutePath}", _))
      threadPools.append(pool)

      val logs = logsInDir(localLogsByDir, dir).values

      val jobsForDir = logs.map { log =>
        val runnable: Runnable = () => {
          // flush the log to ensure latest possible recovery point
          log.flush()
          log.close()
        }
        runnable
      }

      jobs(dir) = jobsForDir.map(pool.submit).toSeq
    }

    try {
      jobs.forKeyValue { (dir, dirJobs) =>
        if (waitForAllToComplete(dirJobs,
          e => warn(s"There was an error in one of the threads during LogManager shutdown: ${e.getCause}"))) {
          val logs = logsInDir(localLogsByDir, dir)

          // update the last flush point
          debug(s"Updating recovery points at $dir")
          checkpointRecoveryOffsetsInDir(dir, logs)

          debug(s"Updating log start offsets at $dir")
          checkpointLogStartOffsetsInDir(dir, logs)

          // mark that the shutdown was clean by creating marker file
          debug(s"Writing clean shutdown marker at $dir")
          CoreUtils.swallow(Files.createFile(new File(dir, LogLoader.CleanShutdownFile).toPath), this)
        }
      }
    } finally {
      threadPools.foreach(_.shutdown())
      // regardless of whether the close succeeded, we need to unlock the data directories
      dirLocks.foreach(_.destroy())
    }

    info("Shutdown complete.")
  }

  /**
   * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
   *
   * @param partitionOffsets Partition logs that need to be truncated
   * @param isFuture True iff the truncation should be performed on the future log of the specified partitions
   */
  def truncateTo(partitionOffsets: Map[TopicPartition, Long], isFuture: Boolean): Unit = {
    val affectedLogs = ArrayBuffer.empty[UnifiedLog]
    for ((topicPartition, truncateOffset) <- partitionOffsets) {
      val log = {
        if (isFuture)
          futureLogs.get(topicPartition)
        else
          currentLogs.get(topicPartition)
      }
      // If the log does not exist, skip it
      if (log != null) {
        // May need to abort and pause the cleaning of the log, and resume after truncation is done.
        val needToStopCleaner = truncateOffset < log.activeSegment.baseOffset
        if (needToStopCleaner && !isFuture)
          abortAndPauseCleaning(topicPartition)
        try {
          if (log.truncateTo(truncateOffset))
            affectedLogs += log
          if (needToStopCleaner && !isFuture)
            maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log, topicPartition)
        } finally {
          if (needToStopCleaner && !isFuture)
            resumeCleaning(topicPartition)
        }
      }
    }

    for (dir <- affectedLogs.map(_.parentDirFile).distinct) {
      checkpointRecoveryOffsetsInDir(dir)
    }
  }

  /**
   * Delete all data in a partition and start the log at the new offset
   *
   * @param topicPartition The partition whose log needs to be truncated
   * @param newOffset The new offset to start the log with
   * @param isFuture True iff the truncation should be performed on the future log of the specified partition
   */
  def truncateFullyAndStartAt(topicPartition: TopicPartition, newOffset: Long, isFuture: Boolean): Unit = {
    val log = {
      if (isFuture)
        futureLogs.get(topicPartition)
      else
        currentLogs.get(topicPartition)
    }
    // If the log does not exist, skip it
    if (log != null) {
      // Abort and pause the cleaning of the log, and resume after truncation is done.
      if (!isFuture)
        abortAndPauseCleaning(topicPartition)
      try {
        log.truncateFullyAndStartAt(newOffset)
        if (!isFuture)
          maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log, topicPartition)
      } finally {
        if (!isFuture)
          resumeCleaning(topicPartition)
      }
      checkpointRecoveryOffsetsInDir(log.parentDirFile)
    }
  }

  /**
   * Write out the current recovery point for all logs to a text file in the log directory
   * to avoid recovering the whole log on startup.
   */
  def checkpointLogRecoveryOffsets(): Unit = {
    val logsByDirCached = logsByDir
    liveLogDirs.foreach { logDir =>
      val logsToCheckpoint = logsInDir(logsByDirCached, logDir)
      checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint)
    }
  }

  /**
   * Write out the current log start offset for all logs to a text file in the log directory
   * to avoid exposing data that have been deleted by DeleteRecordsRequest
   */
  def checkpointLogStartOffsets(): Unit = {
    val logsByDirCached = logsByDir
    liveLogDirs.foreach { logDir =>
      checkpointLogStartOffsetsInDir(logDir, logsInDir(logsByDirCached, logDir))
    }
  }

  /**
   * Checkpoint recovery offsets for all the logs in logDir.
   *
   * @param logDir the directory in which the logs to be checkpointed are
   */
  // Only for testing
  private[log] def checkpointRecoveryOffsetsInDir(logDir: File): Unit = {
    checkpointRecoveryOffsetsInDir(logDir, logsInDir(logDir))
  }

  /**
   * Checkpoint recovery offsets for all the provided logs.
   *
   * @param logDir the directory in which the logs are
   * @param logsToCheckpoint the logs to be checkpointed
   */
  private def checkpointRecoveryOffsetsInDir(logDir: File, logsToCheckpoint: Map[TopicPartition, UnifiedLog]): Unit = {
    try {
      recoveryPointCheckpoints.get(logDir).foreach { checkpoint =>
        val recoveryOffsets = logsToCheckpoint.map { case (tp, log) => tp -> log.recoveryPoint }
        // checkpoint.write calls Utils.atomicMoveWithFallback, which flushes the parent
        // directory and guarantees crash consistency.
        checkpoint.write(recoveryOffsets)
      }
    } catch {
      case e: KafkaStorageException =>
        error(s"Disk error while writing recovery offsets checkpoint in directory $logDir: ${e.getMessage}")
      case e: IOException =>
        logDirFailureChannel.maybeAddOfflineLogDir(logDir.getAbsolutePath,
          s"Disk error while writing recovery offsets checkpoint in directory $logDir: ${e.getMessage}", e)
    }
  }

  /**
   * Checkpoint log start offsets for all the provided logs in the provided directory.
   *
   * @param logDir the directory in which logs are checkpointed
   * @param logsToCheckpoint the logs to be checkpointed
   */
  private def checkpointLogStartOffsetsInDir(logDir: File, logsToCheckpoint: Map[TopicPartition, UnifiedLog]): Unit = {
    try {
      logStartOffsetCheckpoints.get(logDir).foreach { checkpoint =>
        val logStartOffsets = logsToCheckpoint.collect {
          case (tp, log) if log.logStartOffset > log.logSegments.head.baseOffset => tp -> log.logStartOffset
        }
        checkpoint.write(logStartOffsets)
      }
    } catch {
      case e: KafkaStorageException =>
        error(s"Disk error while writing log start offsets checkpoint in directory $logDir: ${e.getMessage}")
    }
  }

  // The logDir should be an absolute path
  def maybeUpdatePreferredLogDir(topicPartition: TopicPartition, logDir: String): Unit = {
    // Do not cache the preferred log directory if either the current log or the future log for this partition exists in the specified logDir
    if (!getLog(topicPartition).exists(_.parentDir == logDir) &&
        !getLog(topicPartition, isFuture = true).exists(_.parentDir == logDir))
      preferredLogDirs.put(topicPartition, logDir)
  }

  /**
   * Abort and pause cleaning of the provided partition and log a message about it.
   */
  def abortAndPauseCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.abortAndPauseCleaning(topicPartition)
      info(s"The cleaning for partition $topicPartition is aborted and paused")
    }
  }

  /**
   * Abort cleaning of the provided partition and log a message about it.
   */
  def abortCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.abortCleaning(topicPartition)
      info(s"The cleaning for partition $topicPartition is aborted")
    }
  }

  /**
   * Resume cleaning of the provided partition and log a message about it.
   */
  private def resumeCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.resumeCleaning(Seq(topicPartition))
      info(s"Cleaning for partition $topicPartition is resumed")
    }
  }

  /**
   * Truncate the cleaner's checkpoint to the based offset of the active segment of
   * the provided log.
   */
  private def maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log: UnifiedLog, topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.maybeTruncateCheckpoint(log.parentDirFile, topicPartition, log.activeSegment.baseOffset)
    }
  }

  /**
   * Get the log if it exists, otherwise return None
   *
   * @param topicPartition the partition of the log
   * @param isFuture True iff the future log of the specified partition should be returned
   */
  def getLog(topicPartition: TopicPartition, isFuture: Boolean = false): Option[UnifiedLog] = {
    if (isFuture)
      Option(futureLogs.get(topicPartition))
    else
      Option(currentLogs.get(topicPartition))
  }

  /**
   * Method to indicate that logs are getting initialized for the partition passed in as argument.
   * This method should always be followed by [[kafka.log.LogManager#finishedInitializingLog]] to indicate that log
   * initialization is done.
   */
  def initializingLog(topicPartition: TopicPartition): Unit = {
    partitionsInitializing(topicPartition) = false
  }

  /**
   * Mark the partition configuration for all partitions that are getting initialized for topic
   * as dirty. That will result in reloading of configuration once initialization is done.
   */
  def topicConfigUpdated(topic: String): Unit = {
    partitionsInitializing.keys.filter(_.topic() == topic).foreach {
      topicPartition => partitionsInitializing.replace(topicPartition, false, true)
    }
  }

  /**
   * Update the configuration of the provided topic.
   */
  def updateTopicConfig(topic: String,
                        newTopicConfig: Properties): Unit = {
    topicConfigUpdated(topic)
    val logs = logsByTopic(topic)
    if (logs.nonEmpty) {
      // Combine the default properties with the overrides in zk to create the new LogConfig
      val newLogConfig = LogConfig.fromProps(currentDefaultConfig.originals, newTopicConfig)
      logs.foreach { log =>
        val oldLogConfig = log.updateConfig(newLogConfig)
        if (oldLogConfig.compact && !newLogConfig.compact) {
          abortCleaning(log.topicPartition)
        }
      }
    }
  }

  /**
   * Mark all in progress partitions having dirty configuration if broker configuration is updated.
   */
  def brokerConfigUpdated(): Unit = {
    partitionsInitializing.keys.foreach {
      topicPartition => partitionsInitializing.replace(topicPartition, false, true)
    }
  }

  /**
   * Method to indicate that the log initialization for the partition passed in as argument is
   * finished. This method should follow a call to [[kafka.log.LogManager#initializingLog]].
   *
   * It will retrieve the topic configs a second time if they were updated while the
   * relevant log was being loaded.
   */
  def finishedInitializingLog(topicPartition: TopicPartition,
                              maybeLog: Option[UnifiedLog]): Unit = {
    val removedValue = partitionsInitializing.remove(topicPartition)
    if (removedValue.contains(true))
      maybeLog.foreach(_.updateConfig(fetchLogConfig(topicPartition.topic)))
  }

  /**
   * If the log already exists, just return a copy of the existing log
   * Otherwise if isNew=true or if there is no offline log directory, create a log for the given topic and the given partition
   * Otherwise throw KafkaStorageException
   *
   * @param topicPartition The partition whose log needs to be returned or created
   * @param isNew Whether the replica should have existed on the broker or not
   * @param isFuture True if the future log of the specified partition should be returned or created
   * @param topicId The topic ID of the partition's topic
   * @throws KafkaStorageException if isNew=false, log is not found in the cache and there is offline log directory on the broker
   * @throws InconsistentTopicIdException if the topic ID in the log does not match the topic ID provided
   */
  def getOrCreateLog(topicPartition: TopicPartition, isNew: Boolean = false, isFuture: Boolean = false, topicId: Option[Uuid]): UnifiedLog = {
    logCreationOrDeletionLock synchronized {
      val log = getLog(topicPartition, isFuture).getOrElse {
        // create the log if it has not already been created in another thread
        if (!isNew && offlineLogDirs.nonEmpty)
          throw new KafkaStorageException(s"Can not create log for $topicPartition because log directories ${offlineLogDirs.mkString(",")} are offline")

        val logDirs: List[File] = {
          val preferredLogDir = preferredLogDirs.get(topicPartition)

          if (isFuture) {
            if (preferredLogDir == null)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition without having a preferred log directory")
            else if (getLog(topicPartition).get.parentDir == preferredLogDir)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition in the current log directory of this partition")
          }

          if (preferredLogDir != null)
            List(new File(preferredLogDir))
          else
            nextLogDirs()
        }

        val logDirName = {
          if (isFuture)
            UnifiedLog.logFutureDirName(topicPartition)
          else
            UnifiedLog.logDirName(topicPartition)
        }

        val logDir = logDirs
          .iterator // to prevent actually mapping the whole list, lazy map
          .map(createLogDirectory(_, logDirName))
          .find(_.isSuccess)
          .getOrElse(Failure(new KafkaStorageException("No log directories available. Tried " + logDirs.map(_.getAbsolutePath).mkString(", "))))
          .get // If Failure, will throw

        val config = fetchLogConfig(topicPartition.topic)
        val log = UnifiedLog(
          dir = logDir,
          config = config,
          logStartOffset = 0L,
          recoveryPoint = 0L,
          maxProducerIdExpirationMs = maxPidExpirationMs,
          producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
          scheduler = scheduler,
          time = time,
          brokerTopicStats = brokerTopicStats,
          logDirFailureChannel = logDirFailureChannel,
          topicId = topicId,
          keepPartitionMetadataFile = keepPartitionMetadataFile)

        if (isFuture)
          futureLogs.put(topicPartition, log)
        else
          currentLogs.put(topicPartition, log)

        info(s"Created log for partition $topicPartition in $logDir with properties ${config.overriddenConfigsAsLoggableString}")
        // Remove the preferred log dir since it has already been satisfied
        preferredLogDirs.remove(topicPartition)

        log
      }
      // When running a ZK controller, we may get a log that does not have a topic ID. Assign it here.
      if (log.topicId.isEmpty) {
        topicId.foreach(log.assignTopicId)
      }

      // Ensure topic IDs are consistent
      topicId.foreach { topicId =>
        log.topicId.foreach { logTopicId =>
          if (topicId != logTopicId)
            throw new InconsistentTopicIdException(s"Tried to assign topic ID $topicId to log for topic partition $topicPartition," +
              s"but log already contained topic ID $logTopicId")
        }
      }
      log
    }
  }

  private[log] def createLogDirectory(logDir: File, logDirName: String): Try[File] = {
    val logDirPath = logDir.getAbsolutePath
    if (isLogDirOnline(logDirPath)) {
      val dir = new File(logDirPath, logDirName)
      try {
        Files.createDirectories(dir.toPath)
        Success(dir)
      } catch {
        case e: IOException =>
          val msg = s"Error while creating log for $logDirName in dir $logDirPath"
          logDirFailureChannel.maybeAddOfflineLogDir(logDirPath, msg, e)
          warn(msg, e)
          Failure(new KafkaStorageException(msg, e))
      }
    } else {
      Failure(new KafkaStorageException(s"Can not create log $logDirName because log directory $logDirPath is offline"))
    }
  }

  /**
   *  Delete logs marked for deletion. Delete all logs for which `currentDefaultConfig.fileDeleteDelayMs`
   *  has elapsed after the delete was scheduled. Logs for which this interval has not yet elapsed will be
   *  considered for deletion in the next iteration of `deleteLogs`. The next iteration will be executed
   *  after the remaining time for the first log that is not deleted. If there are no more `logsToBeDeleted`,
   *  `deleteLogs` will be executed after `currentDefaultConfig.fileDeleteDelayMs`.
   */
  private def deleteLogs(): Unit = {
    var nextDelayMs = 0L
    val fileDeleteDelayMs = currentDefaultConfig.fileDeleteDelayMs
    try {
      def nextDeleteDelayMs: Long = {
        if (!logsToBeDeleted.isEmpty) {
          val (_, scheduleTimeMs) = logsToBeDeleted.peek()
          scheduleTimeMs + fileDeleteDelayMs - time.milliseconds()
        } else
          fileDeleteDelayMs
      }

      while ({nextDelayMs = nextDeleteDelayMs; nextDelayMs <= 0}) {
        val (removedLog, _) = logsToBeDeleted.take()
        if (removedLog != null) {
          try {
            removedLog.delete()
            info(s"Deleted log for partition ${removedLog.topicPartition} in ${removedLog.dir.getAbsolutePath}.")
          } catch {
            case e: KafkaStorageException =>
              error(s"Exception while deleting $removedLog in dir ${removedLog.parentDir}.", e)
          }
        }
      }
    } catch {
      case e: Throwable =>
        error(s"Exception in kafka-delete-logs thread.", e)
    } finally {
      try {
        scheduler.schedule("kafka-delete-logs",
          deleteLogs _,
          delay = nextDelayMs,
          unit = TimeUnit.MILLISECONDS)
      } catch {
        case e: Throwable =>
          if (scheduler.isStarted) {
            // No errors should occur unless scheduler has been shutdown
            error(s"Failed to schedule next delete in kafka-delete-logs thread", e)
          }
      }
    }
  }

  /**
    * Mark the partition directory in the source log directory for deletion and
    * rename the future log of this partition in the destination log directory to be the current log
    *
    * @param topicPartition TopicPartition that needs to be swapped
    */
  def replaceCurrentWithFutureLog(topicPartition: TopicPartition): Unit = {
    logCreationOrDeletionLock synchronized {
      val sourceLog = currentLogs.get(topicPartition)
      val destLog = futureLogs.get(topicPartition)

      info(s"Attempting to replace current log $sourceLog with $destLog for $topicPartition")
      if (sourceLog == null)
        throw new KafkaStorageException(s"The current replica for $topicPartition is offline")
      if (destLog == null)
        throw new KafkaStorageException(s"The future replica for $topicPartition is offline")

      destLog.renameDir(UnifiedLog.logDirName(topicPartition))
      destLog.updateHighWatermark(sourceLog.highWatermark)

      // Now that future replica has been successfully renamed to be the current replica
      // Update the cached map and log cleaner as appropriate.
      futureLogs.remove(topicPartition)
      currentLogs.put(topicPartition, destLog)
      if (cleaner != null) {
        cleaner.alterCheckpointDir(topicPartition, sourceLog.parentDirFile, destLog.parentDirFile)
        resumeCleaning(topicPartition)
      }

      try {
        sourceLog.renameDir(UnifiedLog.logDeleteDirName(topicPartition))
        // Now that replica in source log directory has been successfully renamed for deletion.
        // Close the log, update checkpoint files, and enqueue this log to be deleted.
        sourceLog.close()
        val logDir = sourceLog.parentDirFile
        val logsToCheckpoint = logsInDir(logDir)
        checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint)
        checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint)
        sourceLog.removeLogMetrics()
        addLogToBeDeleted(sourceLog)
      } catch {
        case e: KafkaStorageException =>
          // If sourceLog's log directory is offline, we need close its handlers here.
          // handleLogDirFailure() will not close handlers of sourceLog because it has been removed from currentLogs map
          sourceLog.closeHandlers()
          sourceLog.removeLogMetrics()
          throw e
      }

      info(s"The current replica is successfully replaced with the future replica for $topicPartition")
    }
  }

  /**
    * Rename the directory of the given topic-partition "logdir" as "logdir.uuid.delete" and
    * add it in the queue for deletion.
    *
    * @param topicPartition TopicPartition that needs to be deleted
    * @param isFuture True iff the future log of the specified partition should be deleted
    * @param checkpoint True if checkpoints must be written
    * @return the removed log
    */
  def asyncDelete(topicPartition: TopicPartition,
                  isFuture: Boolean = false,
                  checkpoint: Boolean = true): Option[UnifiedLog] = {
    val removedLog: Option[UnifiedLog] = logCreationOrDeletionLock synchronized {
      removeLogAndMetrics(if (isFuture) futureLogs else currentLogs, topicPartition)
    }
    removedLog match {
      case Some(removedLog) =>
        // We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
        if (cleaner != null && !isFuture) {
          cleaner.abortCleaning(topicPartition)
          if (checkpoint) {
            cleaner.updateCheckpoints(removedLog.parentDirFile, partitionToRemove = Option(topicPartition))
          }
        }
        removedLog.renameDir(UnifiedLog.logDeleteDirName(topicPartition))
        if (checkpoint) {
          val logDir = removedLog.parentDirFile
          val logsToCheckpoint = logsInDir(logDir)
          checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint)
          checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint)
        }
        addLogToBeDeleted(removedLog)
        info(s"Log for partition ${removedLog.topicPartition} is renamed to ${removedLog.dir.getAbsolutePath} and is scheduled for deletion")

      case None =>
        if (offlineLogDirs.nonEmpty) {
          throw new KafkaStorageException(s"Failed to delete log for ${if (isFuture) "future" else ""} $topicPartition because it may be in one of the offline directories ${offlineLogDirs.mkString(",")}")
        }
    }

    removedLog
  }

  /**
   * Rename the directories of the given topic-partitions and add them in the queue for
   * deletion. Checkpoints are updated once all the directories have been renamed.
   *
   * @param topicPartitions The set of topic-partitions to delete asynchronously
   * @param errorHandler The error handler that will be called when a exception for a particular
   *                     topic-partition is raised
   */
  def asyncDelete(topicPartitions: Set[TopicPartition],
                  errorHandler: (TopicPartition, Throwable) => Unit): Unit = {
    val logDirs = mutable.Set.empty[File]

    topicPartitions.foreach { topicPartition =>
      try {
        getLog(topicPartition).foreach { log =>
          logDirs += log.parentDirFile
          asyncDelete(topicPartition, checkpoint = false)
        }
        getLog(topicPartition, isFuture = true).foreach { log =>
          logDirs += log.parentDirFile
          asyncDelete(topicPartition, isFuture = true, checkpoint = false)
        }
      } catch {
        case e: Throwable => errorHandler(topicPartition, e)
      }
    }

    val logsByDirCached = logsByDir
    logDirs.foreach { logDir =>
      if (cleaner != null) cleaner.updateCheckpoints(logDir)
      val logsToCheckpoint = logsInDir(logsByDirCached, logDir)
      checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint)
      checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint)
    }
  }

  /**
   * Provides the full ordered list of suggested directories for the next partition.
   * Currently this is done by calculating the number of partitions in each directory and then sorting the
   * data directories by fewest partitions.
   */
  private def nextLogDirs(): List[File] = {
    if(_liveLogDirs.size == 1) {
      List(_liveLogDirs.peek())
    } else {
      // count the number of logs in each parent directory (including 0 for empty directories
      val logCounts = allLogs.groupBy(_.parentDir).map { case (parent, logs) => parent -> logs.size }
      val zeros = _liveLogDirs.asScala.map(dir => (dir.getPath, 0)).toMap
      val dirCounts = (zeros ++ logCounts).toBuffer

      // choose the directory with the least logs in it
      dirCounts.sortBy(_._2).map {
        case (path: String, _: Int) => new File(path)
      }.toList
    }
  }

  /**
   * Delete any eligible logs. Return the number of segments deleted.
   * Only consider logs that are not compacted.
   */
  def cleanupLogs(): Unit = {
    debug("Beginning log cleanup...")
    var total = 0
    val startMs = time.milliseconds

    // clean current logs.
    val deletableLogs = {
      if (cleaner != null) {
        // prevent cleaner from working on same partitions when changing cleanup policy
        cleaner.pauseCleaningForNonCompactedPartitions()
      } else {
        currentLogs.filter {
          case (_, log) => !log.config.compact
        }
      }
    }

    try {
      deletableLogs.foreach {
        case (topicPartition, log) =>
          debug(s"Garbage collecting '${log.name}'")
          total += log.deleteOldSegments()

          val futureLog = futureLogs.get(topicPartition)
          if (futureLog != null) {
            // clean future logs
            debug(s"Garbage collecting future log '${futureLog.name}'")
            total += futureLog.deleteOldSegments()
          }
      }
    } finally {
      if (cleaner != null) {
        cleaner.resumeCleaning(deletableLogs.map(_._1))
      }
    }

    debug(s"Log cleanup completed. $total files deleted in " +
                  (time.milliseconds - startMs) / 1000 + " seconds")
  }

  /**
   * Get all the partition logs
   */
  def allLogs: Iterable[UnifiedLog] = currentLogs.values ++ futureLogs.values

  def logsByTopic(topic: String): Seq[UnifiedLog] = {
    (currentLogs.toList ++ futureLogs.toList).collect {
      case (topicPartition, log) if topicPartition.topic == topic => log
    }
  }

  /**
   * Map of log dir to logs by topic and partitions in that dir
   */
  private def logsByDir: Map[String, Map[TopicPartition, UnifiedLog]] = {
    // This code is called often by checkpoint processes and is written in a way that reduces
    // allocations and CPU with many topic partitions.
    // When changing this code please measure the changes with org.apache.kafka.jmh.server.CheckpointBench
    val byDir = new mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, UnifiedLog]]()
    def addToDir(tp: TopicPartition, log: UnifiedLog): Unit = {
      byDir.getOrElseUpdate(log.parentDir, new mutable.AnyRefMap[TopicPartition, UnifiedLog]()).put(tp, log)
    }
    currentLogs.foreachEntry(addToDir)
    futureLogs.foreachEntry(addToDir)
    byDir
  }

  private def logsInDir(dir: File): Map[TopicPartition, UnifiedLog] = {
    logsByDir.getOrElse(dir.getAbsolutePath, Map.empty)
  }

  private def logsInDir(cachedLogsByDir: Map[String, Map[TopicPartition, UnifiedLog]],
                        dir: File): Map[TopicPartition, UnifiedLog] = {
    cachedLogsByDir.getOrElse(dir.getAbsolutePath, Map.empty)
  }

  // logDir should be an absolute path
  def isLogDirOnline(logDir: String): Boolean = {
    // The logDir should be an absolute path
    if (!logDirs.exists(_.getAbsolutePath == logDir))
      throw new LogDirNotFoundException(s"Log dir $logDir is not found in the config.")

    _liveLogDirs.contains(new File(logDir))
  }

  /**
   * Flush any log which has exceeded its flush interval and has unwritten messages.
   */
  private def flushDirtyLogs(): Unit = {
    debug("Checking for dirty logs to flush...")

    for ((topicPartition, log) <- currentLogs.toList ++ futureLogs.toList) {
      try {
        val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
        debug(s"Checking if flush is needed on ${topicPartition.topic} flush interval ${log.config.flushMs}" +
              s" last flushed ${log.lastFlushTime} time since last flush: $timeSinceLastFlush")
        if(timeSinceLastFlush >= log.config.flushMs)
          log.flush()
      } catch {
        case e: Throwable =>
          error(s"Error flushing topic ${topicPartition.topic}", e)
      }
    }
  }

  private def removeLogAndMetrics(logs: Pool[TopicPartition, UnifiedLog], tp: TopicPartition): Option[UnifiedLog] = {
    val removedLog = logs.remove(tp)
    if (removedLog != null) {
      removedLog.removeLogMetrics()
      Some(removedLog)
    } else {
      None
    }
  }
}

object LogManager {

  /**
   * Wait all jobs to complete
   * @param jobs jobs
   * @param callback this will be called to handle the exception caused by each Future#get
   * @return true if all pass. Otherwise, false
   */
  private[log] def waitForAllToComplete(jobs: Seq[Future[_]], callback: Throwable => Unit): Boolean = {
    jobs.count(future => Try(future.get) match {
      case Success(_) => false
      case Failure(e) =>
        callback(e)
        true
    }) == 0
  }

  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  val LogStartOffsetCheckpointFile = "log-start-offset-checkpoint"
  val ProducerIdExpirationCheckIntervalMs = 10 * 60 * 1000

  def apply(config: KafkaConfig,
            initialOfflineDirs: Seq[String],
            configRepository: ConfigRepository,
            kafkaScheduler: KafkaScheduler,
            time: Time,
            brokerTopicStats: BrokerTopicStats,
            logDirFailureChannel: LogDirFailureChannel,
            keepPartitionMetadataFile: Boolean): LogManager = {
    //todo:加载默认配置
    val defaultProps = LogConfig.extractLogConfigMap(config)

    LogConfig.validateValues(defaultProps)
    val defaultLogConfig = LogConfig(defaultProps)

    val cleanerConfig = LogCleaner.cleanerConfig(config)

    /**
     * 创建LogManager对象
     * 运行构造器方法
     * todo:内部有一个重要的参数  logDirs=cofnig.logDirs.map(new File(_))
     * 配置了kafka存储目录参数：log.dirs  生产环境当中一般都是配置多个目录
     */
    new LogManager(logDirs = config.logDirs.map(new File(_).getAbsoluteFile),
      initialOfflineDirs = initialOfflineDirs.map(new File(_).getAbsoluteFile),
      configRepository = configRepository,
      initialDefaultConfig = defaultLogConfig,
      cleanerConfig = cleanerConfig,
      recoveryThreadsPerDataDir = config.numRecoveryThreadsPerDataDir,
      flushCheckMs = config.logFlushSchedulerIntervalMs,
      flushRecoveryOffsetCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,
      flushStartOffsetCheckpointMs = config.logFlushStartOffsetCheckpointIntervalMs,
      retentionCheckMs = config.logCleanupIntervalMs,
      maxPidExpirationMs = config.transactionalIdExpirationMs,
      scheduler = kafkaScheduler,
      brokerTopicStats = brokerTopicStats,
      logDirFailureChannel = logDirFailureChannel,
      time = time,
      keepPartitionMetadataFile = keepPartitionMetadataFile,
      interBrokerProtocolVersion = config.interBrokerProtocolVersion)
  }

}
