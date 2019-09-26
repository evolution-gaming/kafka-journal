package com.evolutiongaming.kafka.journal

import cats.Parallel
import cats.effect._
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.catshelper.{FromFuture, FromTry, Log, LogOf, ToFuture, ToTry}
import com.evolutiongaming.kafka.journal.eventual.cassandra._
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.scassandra.{AuthenticationConfig, CassandraClusterOf, CassandraConfig}
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.producer.{Acks, ProducerConfig}
import com.evolutiongaming.smetrics.MeasureDuration

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.concurrent.duration._

object ReadEventsApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    implicit val parallel = IO.ioParallel
    implicit val executor = ExecutionContext.global
    runF[IO](executor).as(ExitCode.Success)
  }

  private def runF[F[_] : ConcurrentEffect : ContextShift : Timer : Clock : FromFuture : ToFuture : Parallel : FromGFuture : FromTry : ToTry](
    executor: ExecutionContextExecutor,
  ): F[Unit] = {

    for {
      logOf  <- LogOf.slf4j[F]
      log    <- logOf(ReadEventsApp.getClass)
      result <- {
        implicit val logOf1 = logOf
        implicit val measureDuration = MeasureDuration.fromClock(Clock[F])
        implicit val fromAttempt = FromAttempt.lift[F]
        implicit val fromJsResult = FromJsResult.lift[F]
        runF[F](executor, log).handleErrorWith { error =>
          log.error(s"failed with $error", error)
        }
      }
    } yield result

  }

  private def runF[F[_] : ConcurrentEffect : ContextShift : Timer : Clock : FromFuture : ToFuture : Parallel : LogOf : FromGFuture : MeasureDuration : FromTry : ToTry : FromAttempt : FromJsResult](
    executor: ExecutionContextExecutor,
    log: Log[F],
  ): F[Unit] = {

    implicit val kafkaConsumerOf = KafkaConsumerOf[F](executor)

    implicit val kafkaProducerOf = KafkaProducerOf[F](executor)

    implicit val randomId = RandomId.uuid[F]

    val commonConfig = CommonConfig(
      clientId = "ReadEventsApp".some,
      bootstrapServers = Nel.of("localhost:9092"))

    val producerConfig = ProducerConfig(
      common = commonConfig,
      idempotence = true,
      acks = Acks.All)

    val consumerConfig = ConsumerConfig(common = commonConfig)

    val consumer = Journal.Consumer.of[F](consumerConfig, 100.millis)

    val eventualCassandraConfig = EventualCassandraConfig(
      schema = SchemaConfig(
        keyspace = SchemaConfig.Keyspace(
          name = "keyspace",
          autoCreate = false),
        autoCreate = false),
      client = CassandraConfig(
        contactPoints = com.evolutiongaming.nel.Nel("127.0.0.1"),
        authentication = Some(AuthenticationConfig(
          username = "username",
          password = "password"))))

    val journal = for {
      cassandraClusterOf <- Resource.liftF(CassandraClusterOf.of[F])
      origin             <- Resource.liftF(Origin.hostName[F])
      eventualJournal    <- EventualCassandra.of[F](eventualCassandraConfig, origin, none, cassandraClusterOf)
      headCache          <- HeadCache.of[F](consumerConfig, eventualJournal, none)
      producer           <- Journal.Producer.of[F](producerConfig)
    } yield {
      val origin = Origin("ReadEventsApp")
      val journal = Journal[F](origin.some, producer, consumer, eventualJournal, headCache, log)
      val key = Key(id = "id", topic = "topic")
      for {
        pointer <- journal.pointer(key)
        seqNrs  <- journal.read(key).map(_.seqNr).toList
        _       <- log.info(s"pointer: $pointer")
        _       <- log.info(s"seqNrs: $seqNrs")
      } yield {}
    }

    journal.use(identity)
  }
}
