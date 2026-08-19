package com.evolution.kafka.journal.app

import cats.data.NonEmptyList as Nel
import cats.effect.*
import cats.effect.implicits.*
import cats.implicits.*
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.Journal.DataIntegrityConfig
import com.evolution.kafka.journal.TestJsonCodec.instance
import com.evolution.kafka.journal.cassandra.KeyspaceConfig
import com.evolution.kafka.journal.eventual.cassandra.*
import com.evolutiongaming.catshelper.*
import com.evolutiongaming.scassandra.{AuthenticationConfig, CassandraClusterOf, CassandraConfig}
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.producer.{Acks, ProducerConfig}

import scala.concurrent.duration.*

object ReadEventsApp extends IOApp {
  import cats.effect.unsafe.implicits.global

  def run(args: List[String]): IO[ExitCode] = {
    for {
      logOf <- LogOf.slf4j[IO]
      log <- logOf(ReadEventsApp.getClass)
      _ <- {
        implicit val logOf1: LogOf[IO] = logOf
        runIo(log).handleErrorWith { error =>
          log.error(s"failed with $error", error)
        }
      }
    } yield ExitCode.Success
  }

  private def runIo(
    log: Log[IO],
  )(implicit
    logOf: LogOf[IO],
  ): IO[Unit] = {
    implicit val kafkaConsumerOf: KafkaConsumerOf[IO] = KafkaConsumerOf[IO]()
    implicit val kafkaProducerOf: KafkaProducerOf[IO] = KafkaProducerOf[IO]()
    implicit val randomIdOf: RandomIdOf[IO] = RandomIdOf.uuid

    val commonConfig = CommonConfig(clientId = "ReadEventsApp".some, bootstrapServers = Nel.of("localhost:9092"))

    val producerConfig = ProducerConfig(common = commonConfig, idempotence = true, acks = Acks.All)

    val consumerConfig = ConsumerConfig(common = commonConfig)

    val consumer = Journals.Consumer.make[IO](consumerConfig, 100.millis)

    val eventualCassandraConfig = EventualCassandraConfig(
      schema = SchemaConfig(keyspace = KeyspaceConfig(name = "keyspace", autoCreate = false), autoCreate = false),
      client = CassandraConfig(
        contactPoints = com.evolutiongaming.nel.Nel("127.0.0.1"),
        authentication = AuthenticationConfig(username = "username", password = "password").some,
      ),
    )

    val journal = for {
      cassandraClusterOf <- CassandraClusterOf.of[IO].toResource
      origin <- Origin.hostName[IO].toResource
      eventualJournal <- EventualCassandra.make[IO](
        eventualCassandraConfig,
        origin,
        none,
        cassandraClusterOf,
        DataIntegrityConfig.Default,
      )
      headCache <- HeadCache.make[IO](consumerConfig, eventualJournal, none)
      producer <- Journals.Producer.make[IO](producerConfig)
    } yield {
      val origin = Origin("ReadEventsApp")
      val journals = Journals[IO](origin.some, producer, consumer, eventualJournal, headCache, log, none)
      val key = Key(id = "id", topic = "topic")
      val journal = journals(key)
      for {
        pointer <- journal.pointer
        seqNrs <- journal.read().map(_.seqNr).toList
        _ <- log.info(s"pointer: $pointer")
        _ <- log.info(s"seqNrs: $seqNrs")
      } yield {}
    }

    journal.use(identity)
  }
}
