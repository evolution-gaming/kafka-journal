package com.evolutiongaming.kafka.journal.replicator

import java.util.UUID

import akka.actor.ActorSystem
import com.evolutiongaming.cassandra.{CassandraConfig, CreateCluster}
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.cassandra.{EventualCassandraConfig, ReplicatedCassandra, SchemaConfig}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{Bytes => _}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

trait Replicator {
  def shutdown(): Async[Unit]
}

// TODO refactor EventualDb api, make sure it does operate with Seq[Actions]
object Replicator {

  def apply(implicit system: ActorSystem, ec: ExecutionContext): Replicator = {
    val ecBlocking = system.dispatchers.lookup("kafka-replicator-blocking-dispatcher")
    apply(ecBlocking)
  }

  def apply(ecBlocking: ExecutionContext)(implicit system: ActorSystem, ec: ExecutionContext): Replicator = {
    val log = ActorLog(system, Replicator.getClass)
    val topics = Nel("journal", "consistency", "perf", "integration")
    val cassandraConfig = CassandraConfig.Default
    val cluster = CreateCluster(cassandraConfig)
    val session = Await.result(cluster.connect(), 5.seconds) // TODO handle this properly
    val schemaConfig = SchemaConfig.Default
    val config = EventualCassandraConfig.Default
    val journal = ReplicatedCassandra(session, schemaConfig, config)

    val replicators = for {
      topic <- topics
    } yield {
      val groupId = UUID.randomUUID().toString
      val consumerConfig = ConsumerConfig.Default.copy(
        groupId = Some(groupId),
        autoOffsetReset = AutoOffsetReset.Earliest)
      val consumer = CreateConsumer[String, Bytes](consumerConfig, ecBlocking)

      val log = ActorLog(system, TopicReplicator.getClass) prefixed topic
      TopicReplicator(topic, consumer, journal, log)
    }

    new Replicator {
      def shutdown() = {
        val shutdowns = replicators.toList.map(_.shutdown())
        for {
          _ <- Async.foldUnit(shutdowns)
          _ <- cluster.close().async
        } yield {}
      }
    }
  }
}
