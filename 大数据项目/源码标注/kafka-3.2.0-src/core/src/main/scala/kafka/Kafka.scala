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

package kafka

import java.util.Properties

import joptsimple.OptionParser
import kafka.server.{KafkaConfig, KafkaRaftServer, KafkaServer, Server}
import kafka.utils.Implicits._
import kafka.utils.{CommandLineUtils, Exit, Logging}
import org.apache.kafka.common.utils.{Java, LoggingSignalHandler, OperatingSystem, Time, Utils}

import scala.jdk.CollectionConverters._

/**
 * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
 *  注释： Kafka server Scala
 *  class Kafka
 *  object Kafka
 */
object Kafka extends Logging {

    def getPropsFromArgs(args: Array[String]): Properties = {
        
        val optionParser = new OptionParser(false)

        val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file")
            .withRequiredArg()
            .ofType(classOf[String])

        // This is just to make the parameter show up in the help output, we are not actually using this due the
        // fact that this class ignores the first parameter which is interpreted as positional and mandatory
        // but would not be mandatory if --version is specified
        // This is a bit of an ugly crutch till we get a chance to rework the entire command line parsing
        optionParser.accepts("version", "Print version information and exit.")

        if (args.length == 0 || args.contains("--help")) {
            CommandLineUtils.printUsageAndDie(optionParser,
                "USAGE: java [options] %s server.properties [--override property=value]*".format(this.getClass.getCanonicalName.split('$').head))
        }

        if (args.contains("--version")) {
            CommandLineUtils.printVersionAndDie()
        }

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 加载配置
         *  通过 Properties 加载 server.properties 配置文件内容
         */
        val props = Utils.loadProps(args(0))

        if (args.length > 1) {
            val options = optionParser.parse(args.slice(1, args.length): _*)

            if (options.nonOptionArguments().size() > 0) {
                CommandLineUtils.printUsageAndDie(optionParser, "Found non argument parameters: " + options.nonOptionArguments().toArray.mkString(","))
            }

            props ++= CommandLineUtils.parseKeyValueArgs(options.valuesOf(overrideOpt).asScala)
        }
        props
    }

    private def buildServer(props: Properties): Server = {

        // TODO_MA 马中华 注释： 根据用户的配置来决定是不是要启动 基于 Raft 模式的 kafka 集群
        val config = KafkaConfig.fromProps(props, false)

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 基于 Zookeeper 的运行模式
         */
        if (config.requiresZookeeper) {
            new KafkaServer(
                config,
                Time.SYSTEM,
                threadNamePrefix = None,
                enableForwarding = false
            )
        }

        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *  注释： 基于 KRaft 的运行模式
         */
        else {
            new KafkaRaftServer(
                config,
                Time.SYSTEM,
                threadNamePrefix = None
            )
        }
    }

    /**
     * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
     *  注释： 核心：
     *  1、 解析 server.properties（通过 Properties API）
     *  2、 构建 KafkaServer（特别需要注意；成员变量特别多）
     *  3、 启动 kafkaServer
     */
    def main(args: Array[String]): Unit = {
        try {

            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 第一步： 通过 Properties 加载 server.properties 配置文件内容
             *  args[0] = server.properties
             */
            val serverProps = getPropsFromArgs(args)

            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 第二步： 构建 KafkaSever
             *  创建两种类型的 RPC 服务端：创建 SocketServer
             *  1、datapalne
             *  2、controllerplane
             *  KafkaServer 这个实例
             */
            val server = buildServer(serverProps)

            try {
                if (!OperatingSystem.IS_WINDOWS && !Java.isIbmJdk)
                    new LoggingSignalHandler().register()
            } catch {
                case e: ReflectiveOperationException =>
                    warn("Failed to register optional signal handler that logs a message when the process is terminated " +
                        s"by a signal. Reason for registration failure is: $e", e)
            }

            // attach shutdown handler to catch terminating signals as well as normal termination
            Exit.addShutdownHook("kafka-shutdown-hook", {
                try server.shutdown()
                catch {
                    case _: Throwable =>
                        fatal("Halting Kafka.")
                        // Calling exit() can lead to deadlock as exit() can be called multiple times. Force exit.
                        Exit.halt(1)
                }
            })

            /**
             * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
             *  注释： 第三步： 启动 kafkaServer
             *  kafkaServer 启动： 启动内部的 SocketServer
             *  内部实现，总共按照我的总结：47 个步骤
             */
            try server.startup()

            catch {
                case _: Throwable =>
                    // KafkaServer.startup() calls shutdown() in case of exceptions, so we invoke `exit` to set the status code
                    fatal("Exiting Kafka.")
                    Exit.exit(1)
            }
            server.awaitShutdown()
        }
        catch {
            case e: Throwable =>
                fatal("Exiting Kafka due to fatal exception", e)
                Exit.exit(1)
        }
        Exit.exit(0)
    }
}
