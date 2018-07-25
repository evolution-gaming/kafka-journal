package com.evolutiongaming.kafka

import java.net.InetSocketAddress

import com.evolutiongaming.tmpdir.TmpDir
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.zookeeper.server.{ServerCnxnFactory, SetShutdownHandler, ZooKeeperServer}

import scala.collection.JavaConverters._

object StartKafka {

  type Shutdown = () => Unit

  object Shutdown {
    val Empty: Shutdown = () => ()
  }

  def apply(
    kafkaAddress: Address = Address.Kafka,
    zkAddress: Address = Address.ZooKeeper): Shutdown = {

    val shutdownZk = zooKeeperServer(zkAddress)
    val shutdownKafka = kafkaServer(kafkaAddress = kafkaAddress, zkAddress = zkAddress)

    () => {
      shutdownKafka()
      shutdownZk()
    }
  }

  private def zooKeeperServer(address: Address) = {
    val tmpDir = TmpDir("zookeeper-")
    val inetAddress = new InetSocketAddress(address.host, address.port)
    val server = new ZooKeeperServer(tmpDir.file, tmpDir.file, ZooKeeperServer.DEFAULT_TICK_TIME)
    SetShutdownHandler(server)
    val factory = ServerCnxnFactory.createFactory
    factory.configure(inetAddress, 1024)
    factory.startup(server)
    () => {
      factory.shutdown()
      tmpDir.delete()
    }
  }

  private def kafkaServer(kafkaAddress: Address, zkAddress: Address) = {
    val listener = s"${ SecurityProtocol.PLAINTEXT }://$kafkaAddress"
    val tmpDir = TmpDir("kafka-")
    val props = Map[String, String](
      KafkaConfig.HostNameProp -> kafkaAddress.host,
      KafkaConfig.ZkConnectProp -> zkAddress.toString,
      KafkaConfig.BrokerIdProp -> 0.toString,
      KafkaConfig.ListenersProp -> listener,
      KafkaConfig.AdvertisedListenersProp -> listener,
      KafkaConfig.AutoCreateTopicsEnableProp -> true.toString,
      KafkaConfig.LogDirProp -> tmpDir.file.getAbsolutePath,
      KafkaConfig.LogFlushIntervalMessagesProp -> 1.toString,
      KafkaConfig.OffsetsTopicReplicationFactorProp -> 1.toString,
      KafkaConfig.OffsetsTopicPartitionsProp -> 1.toString,
      KafkaConfig.TransactionsTopicReplicationFactorProp -> 1.toString,
      KafkaConfig.TransactionsTopicMinISRProp -> 1.toString,
      KafkaConfig.LogCleanerDedupeBufferSizeProp -> (10 * 1024L * 1024).toString,
      KafkaConfig.GroupInitialRebalanceDelayMsProp -> 0.toString)

    val config = new KafkaConfig(props.asJava)
    val server = new KafkaServer(config)
    server.startup()

    () => {
      server.shutdown()
      server.awaitShutdown()
      tmpDir.delete()
    }
  }
}

final case class Address(host: String = "localhost", port: Int) {
  override def toString: String = s"$host:$port"
}

object Address {
  val Kafka: Address = Address(port = 9092)
  val ZooKeeper: Address = Address(port = 2181)
}