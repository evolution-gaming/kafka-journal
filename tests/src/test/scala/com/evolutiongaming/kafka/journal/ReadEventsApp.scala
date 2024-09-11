package com.evolutiongaming.kafka.journal

import cats.Parallel
import cats.data.NonEmptyList as Nel
import cats.effect.*
import cats.effect.syntax.resource.*
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{RandomIdOf, RandomIdOf as _, *}
import com.evolutiongaming.kafka.journal.Journal.DataIntegrityConfig
import com.evolutiongaming.kafka.journal.TestJsonCodec.instance
import com.evolutiongaming.kafka.journal.cassandra.KeyspaceConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.*
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.scassandra.{AuthenticationConfig, CassandraClusterOf, CassandraConfig}
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.producer.{Acks, ProducerConfig}

import scala.concurrent.duration.*

object ReadEventsApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    import cats.effect.unsafe.implicits.global
    runF[IO].as(ExitCode.Success)
  }

  private def runF[F[_]: Async: ToFuture: Parallel: FromGFuture: FromTry: ToTry: Fail]: F[Unit] = {

    for {
      logOf <- LogOf.slf4j[F]
      log   <- logOf(ReadEventsApp.getClass)
      result <- {
        implicit val logOf1          = logOf
        implicit val measureDuration = MeasureDuration.fromClock(Clock[F])
        implicit val fromAttempt     = FromAttempt.lift[F]
        implicit val fromJsResult    = FromJsResult.lift[F]
        runF[F](log).handleErrorWith { error =>
          log.error(s"failed with $error", error)
        }
      }
    } yield result

  }

  private def runF[F[
    _,
  ]: Async: ToFuture: Parallel: LogOf: FromGFuture: MeasureDuration: FromTry: ToTry: FromAttempt: FromJsResult: Fail](
    log: Log[F],
  ): F[Unit] = {

    implicit val kafkaConsumerOf = KafkaConsumerOf[F]()

    implicit val kafkaProducerOf = KafkaProducerOf[F]()

    implicit val randomIdOf = RandomIdOf.uuid[F]

    val commonConfig = CommonConfig(clientId = "ReadEventsApp".some, bootstrapServers = Nel.of("localhost:9092"))

    val producerConfig = ProducerConfig(common = commonConfig, idempotence = true, acks = Acks.All)

    val consumerConfig = ConsumerConfig(common = commonConfig)

    val consumer = Journals.Consumer.of[F](consumerConfig, 100.millis)

    val eventualCassandraConfig = EventualCassandraConfig(
      schema = SchemaConfig(keyspace = KeyspaceConfig(name = "keyspace", autoCreate = false), autoCreate = false),
      client = CassandraConfig(
        contactPoints  = com.evolutiongaming.nel.Nel("127.0.0.1"),
        authentication = AuthenticationConfig(username = "username", password = "password").some,
      ),
    )

    val journal = for {
      cassandraClusterOf <- CassandraClusterOf.of[F].toResource
      origin             <- Origin.hostName[F].toResource
      eventualJournal <- EventualCassandra
        .of1[F](eventualCassandraConfig, origin, none, cassandraClusterOf, DataIntegrityConfig.Default)
      headCache <- HeadCache.of[F](consumerConfig, eventualJournal, none)
      producer  <- Journals.Producer.of[F](producerConfig)
    } yield {
      val origin   = Origin("ReadEventsApp")
      val journals = Journals[F](origin.some, producer, consumer, eventualJournal, headCache, log, none)
      val key      = Key(id = "id", topic = "topic")
      val journal  = journals(key)
      for {
        pointer <- journal.pointer
        seqNrs  <- journal.read().map(_.seqNr).toList
        _       <- log.info(s"pointer: $pointer")
        _       <- log.info(s"seqNrs: $seqNrs")
      } yield {}
    }

    journal.use(identity)
  }
}
