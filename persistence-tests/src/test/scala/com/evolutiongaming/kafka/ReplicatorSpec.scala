package com.evolutiongaming.kafka

import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import com.evolutiongaming.cassandra.Helpers._
import com.evolutiongaming.cassandra.{CassandraConfig, CreateCluster, StartCassandra}
import com.evolutiongaming.kafka.journal.ally.cassandra.{AllyCassandra, SchemaConfig}
import com.evolutiongaming.kafka.journal.ally.{AllyRecord, PartitionOffset}
import com.evolutiongaming.safeakka.actor.ActorLog
import org.scalatest.{Matchers, WordSpec}

import scala.compat.Platform
import scala.concurrent.Await
import scala.concurrent.duration.{FiniteDuration, _}

class ReplicatorSpec extends WordSpec with Matchers {

  "Replicator" should {

    "replicate kafka records to cassandra" ignore {

      implicit val system = ActorSystem("ReplicatorSpec")
      implicit val ec = system.dispatcher

      val log = ActorLog(system, classOf[ReplicatorSpec])

      val shutdownCassandra = StartCassandra()
      val schemaConfig = SchemaConfig.Default

      /*def startReplicator() = {
        val future = Future {
          val groupId = UUID.randomUUID().toString
          val config = ConsumerConfig.Default.copy(
            groupId = Some(groupId),
            autoOffsetReset = AutoOffsetReset.Earliest)
          val consumer = CreateConsumer[String, Bytes](config)
          Replicator(consumer)
        }.flatten

        future.failed.foreach { failure =>
          log.error(s"Replicator failed: $failure", failure)
        }
      }*/

      val partitionSize = 500000 // TODO

      case class Record(seqNr: Long, value: String, timestamp: Long, tags: Set[String])

      val persistenceId = "persistenceId"

      val records = for {
        seqNr <- 1L to 10
      } yield {

        val payload = seqNr.toString.getBytes(StandardCharsets.UTF_8)
        AllyRecord(
          id = persistenceId,
          seqNr = seqNr,
          Platform.currentTime,
          payload = payload,
          Set(seqNr.toString),
          PartitionOffset(1, 1))
      }

      try {
        val cassandraConfig = CassandraConfig.Default
        val cluster = CreateCluster(cassandraConfig)
        val result = for {
          session <- cluster.connectAsync().asScala()
          allyDb = AllyCassandra(session, schemaConfig)
          _ <- allyDb.save(records)
          record <- allyDb.last(persistenceId)
        } yield {
          record.map { _.seqNr }
        }

        val seqNr = Await.result(result, 1.minute)
        seqNr shouldEqual Some(10)
      } finally {
        shutdownCassandra()
        system.terminate()
      }
    }
  }
}

object ReplicatorSpec {

  def createTable(name: String, gcGrace: FiniteDuration = 10.days) =
    s"""
      CREATE TABLE IF NOT EXISTS $name (
        id text,
        partition bigint,
        seq_nr bigint,
        timestamp timestamp,
        payload blob,
        tags set<text>,
        PRIMARY KEY ((id, partition), seq_nr, timestamp)) """
  //        WITH gc_grace_seconds =${gcGrace.toSeconds} TODO
  //        AND compaction = ${config.tableCompactionStrategy.asCQL}

  def selectHighestSequenceNr(tableName: String) =
    s"""
     SELECT seq_nr FROM $tableName WHERE
       id = ? AND
       partition = ?
       ORDER BY seq_nr
       DESC LIMIT 1
   """
}