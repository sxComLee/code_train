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

import kafka.cluster.BrokerEndPoint
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Implicits._
import kafka.utils.Logging
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.utils.Utils

import scala.collection.{Map, Set, mutable}

abstract class AbstractFetcherManager[T <: AbstractFetcherThread](val name: String, clientId: String, numFetchers: Int)
    extends Logging with KafkaMetricsGroup {

    // map of (source broker_id, fetcher_id per source broker) => fetcher.
    // package private for test
    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 用来管理 AbstractFetcherThread，具体实现是：ReplicaFetcherThread
     *  该组件提供了三个方法来维护这个集合：
     *  1、addFetcherForPartitions() 让 Follower Replica 从指定的 Offset 到 Leader Replia 进行同步
     *  2、removeFetcherForPartitions() 停止指定 Follower Replica 的数据同步
     *  3、shutdownIdleFetcherThreads() 停止闲置的 Fetcher（不再为任何 Partition 提供数据同步） 线程
     */
    private[server] val fetcherThreadMap = new mutable.HashMap[BrokerIdAndFetcherId, T]

    private val lock = new Object
    private var numFetchersPerBroker = numFetchers
    val failedPartitions = new FailedPartitions
    this.logIdent = "[" + name + "] "

    private val tags = Map("clientId" -> clientId)

    newGauge("MaxLag", () => {
        // current max lag across all fetchers/topics/partitions
        fetcherThreadMap.values.foldLeft(0L) { (curMaxLagAll, fetcherThread) =>
            val maxLagThread = fetcherThread.fetcherLagStats.stats.values.foldLeft(0L)((curMaxLagThread, lagMetrics) =>
                math.max(curMaxLagThread, lagMetrics.lag))
            math.max(curMaxLagAll, maxLagThread)
        }
    }, tags)

    newGauge("MinFetchRate", () => {
        // current min fetch rate across all fetchers/topics/partitions
        val headRate = fetcherThreadMap.values.headOption.map(_.fetcherStats.requestRate.oneMinuteRate).getOrElse(0.0)
        fetcherThreadMap.values.foldLeft(headRate)((curMinAll, fetcherThread) =>
            math.min(curMinAll, fetcherThread.fetcherStats.requestRate.oneMinuteRate))
    }, tags)

    newGauge("FailedPartitionsCount", () => failedPartitions.size, tags)

    newGauge("DeadThreadCount", () => deadThreadCount, tags)

    private[server] def deadThreadCount: Int = lock synchronized { fetcherThreadMap.values.count(_.isThreadFailed) }

    def resizeThreadPool(newSize: Int): Unit = {
        def migratePartitions(newSize: Int): Unit = {
            val allRemovedPartitionsMap = mutable.Map[TopicPartition, InitialFetchState]()
            fetcherThreadMap.forKeyValue { (id, thread) =>
                val partitionStates = thread.removeAllPartitions()
                if (id.fetcherId >= newSize)
                    thread.shutdown()
                partitionStates.forKeyValue { (topicPartition, currentFetchState) =>
                    val initialFetchState = InitialFetchState(currentFetchState.topicId, thread.sourceBroker,
                        currentLeaderEpoch = currentFetchState.currentLeaderEpoch,
                        initOffset = currentFetchState.fetchOffset)
                    allRemovedPartitionsMap += topicPartition -> initialFetchState
                }
            }
            // failed partitions are removed when adding partitions to fetcher
            addFetcherForPartitions(allRemovedPartitionsMap)
        }

        lock synchronized {
            val currentSize = numFetchersPerBroker
            info(s"Resizing fetcher thread pool size from $currentSize to $newSize")
            numFetchersPerBroker = newSize
            if (newSize != currentSize) {
                // We could just migrate some partitions explicitly to new threads. But this is currently
                // reassigning all partitions using the new thread size so that hash-based allocation
                // works with partition add/delete as it did before.
                migratePartitions(newSize)
            }
            shutdownIdleFetcherThreads()
        }
    }

    // Visible for testing
    private[server] def getFetcher(topicPartition: TopicPartition): Option[T] = {
        lock synchronized {
            fetcherThreadMap.values.find { fetcherThread =>
                fetcherThread.fetchState(topicPartition).isDefined
            }
        }
    }

    // Visibility for testing
    private[server] def getFetcherId(topicPartition: TopicPartition): Int = {
        lock synchronized {
            Utils.abs(31 * topicPartition.topic.hashCode() + topicPartition.partition) % numFetchersPerBroker
        }
    }

    // This method is only needed by ReplicaAlterDirManager
    def markPartitionsForTruncation(brokerId: Int, topicPartition: TopicPartition, truncationOffset: Long): Unit = {
        lock synchronized {
            val fetcherId = getFetcherId(topicPartition)
            val brokerIdAndFetcherId = BrokerIdAndFetcherId(brokerId, fetcherId)
            fetcherThreadMap.get(brokerIdAndFetcherId).foreach { thread =>
                thread.markPartitionsForTruncation(topicPartition, truncationOffset)
            }
        }
    }

    // to be defined in subclass to create a specific fetcher
    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 抽象方法
     */
    def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): T

    def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit = {
        lock synchronized {
            val partitionsPerFetcher = partitionAndOffsets.groupBy { case (topicPartition, brokerAndInitialFetchOffset) =>
                BrokerAndFetcherId(brokerAndInitialFetchOffset.leader, getFetcherId(topicPartition))
            }

            def addAndStartFetcherThread(brokerAndFetcherId: BrokerAndFetcherId,
                brokerIdAndFetcherId: BrokerIdAndFetcherId
            ): T = {
                /**
                 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
                 *  注释：
                 */
                val fetcherThread = createFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker)
                fetcherThreadMap.put(brokerIdAndFetcherId, fetcherThread)
                fetcherThread.start()
                fetcherThread
            }

            for ((brokerAndFetcherId, initialFetchOffsets) <- partitionsPerFetcher) {
                val brokerIdAndFetcherId = BrokerIdAndFetcherId(brokerAndFetcherId.broker.id, brokerAndFetcherId.fetcherId)
                val fetcherThread = fetcherThreadMap.get(brokerIdAndFetcherId) match {
                    case Some(currentFetcherThread) if currentFetcherThread.sourceBroker == brokerAndFetcherId.broker =>
                        // reuse the fetcher thread
                        currentFetcherThread
                    case Some(f) =>
                        f.shutdown()
                        addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
                    case None =>
                        addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
                }
                // failed partitions are removed when added partitions to thread
                addPartitionsToFetcherThread(fetcherThread, initialFetchOffsets)
            }
        }
    }

    def addFailedPartition(topicPartition: TopicPartition): Unit = {
        lock synchronized {
            failedPartitions.add(topicPartition)
        }
    }

    protected def addPartitionsToFetcherThread(fetcherThread: T,
        initialOffsetAndEpochs: collection.Map[TopicPartition, InitialFetchState]
    ): Unit = {
        fetcherThread.addPartitions(initialOffsetAndEpochs)
        info(s"Added fetcher to broker ${fetcherThread.sourceBroker.id} for partitions $initialOffsetAndEpochs")
    }

    /**
     * If the fetcher and partition state exist, update all to include the topic ID
     *
     * @param partitionsToUpdate a mapping of partitions to be updated to their leader IDs
     * @param topicIds           the mappings from topic name to ID or None if it does not exist
     */
    def maybeUpdateTopicIds(partitionsToUpdate: Map[TopicPartition, Int], topicIds: String => Option[Uuid]): Unit = {
        lock synchronized {
            val partitionsPerFetcher = partitionsToUpdate.groupBy { case (topicPartition, leaderId) =>
                BrokerIdAndFetcherId(leaderId, getFetcherId(topicPartition))
            }.map { case (brokerAndFetcherId, partitionsToUpdate) =>
                (brokerAndFetcherId, partitionsToUpdate.keySet)
            }

            for ((brokerIdAndFetcherId, partitions) <- partitionsPerFetcher) {
                fetcherThreadMap.get(brokerIdAndFetcherId).foreach(_.maybeUpdateTopicIds(partitions, topicIds))
            }
        }
    }

    def removeFetcherForPartitions(partitions: Set[TopicPartition]): Map[TopicPartition, PartitionFetchState] = {
        val fetchStates = mutable.Map.empty[TopicPartition, PartitionFetchState]
        lock synchronized {
            for (fetcher <- fetcherThreadMap.values)
                fetchStates ++= fetcher.removePartitions(partitions)
            failedPartitions.removeAll(partitions)
        }
        if (partitions.nonEmpty)
            info(s"Removed fetcher for partitions $partitions")
        fetchStates
    }

    def shutdownIdleFetcherThreads(): Unit = {
        lock synchronized {
            val keysToBeRemoved = new mutable.HashSet[BrokerIdAndFetcherId]
            for ((key, fetcher) <- fetcherThreadMap) {
                if (fetcher.partitionCount <= 0) {
                    fetcher.shutdown()
                    keysToBeRemoved += key
                }
            }
            fetcherThreadMap --= keysToBeRemoved
        }
    }

    def closeAllFetchers(): Unit = {
        lock synchronized {
            for ((_, fetcher) <- fetcherThreadMap) {
                fetcher.initiateShutdown()
            }

            for ((_, fetcher) <- fetcherThreadMap) {
                fetcher.shutdown()
            }
            fetcherThreadMap.clear()
        }
    }
}

/**
 * The class FailedPartitions would keep a track of partitions marked as failed either during truncation or appending
 * resulting from one of the following errors -
 * <ol>
 *   <li> Storage exception
 *   <li> Fenced epoch
 *   <li> Unexpected errors
 * </ol>
 * The partitions which fail due to storage error are eventually removed from this set after the log directory is
 * taken offline.
 */
class FailedPartitions {
    private val failedPartitionsSet = new mutable.HashSet[TopicPartition]

    def size: Int = synchronized {
        failedPartitionsSet.size
    }

    def add(topicPartition: TopicPartition): Unit = synchronized {
        failedPartitionsSet += topicPartition
    }

    def removeAll(topicPartitions: Set[TopicPartition]): Unit = synchronized {
        failedPartitionsSet --= topicPartitions
    }

    def contains(topicPartition: TopicPartition): Boolean = synchronized {
        failedPartitionsSet.contains(topicPartition)
    }

    def partitions(): Set[TopicPartition] = synchronized {
        failedPartitionsSet.toSet
    }
}

case class BrokerAndFetcherId(broker: BrokerEndPoint, fetcherId: Int)

case class InitialFetchState(topicId: Option[Uuid], leader: BrokerEndPoint, currentLeaderEpoch: Int, initOffset: Long)

case class BrokerIdAndFetcherId(brokerId: Int, fetcherId: Int)
