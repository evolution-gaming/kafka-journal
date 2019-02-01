package com.evolutiongaming.kafka.journal

import akka.actor.ActorSystem
import cats.effect._
import cats.implicits._
import com.evolutiongaming.kafka.journal.eventual.cassandra._
import com.evolutiongaming.kafka.journal.util.{ActorSystemOf, FromFuture, Par, ToFuture}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.scassandra.{AuthenticationConfig, CassandraConfig}
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.producer.ProducerConfig

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object ReadEventsApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val system = ActorSystem("ReadEventsApp")
    implicit val ec = system.dispatcher
    implicit val timer = IO.timer(ec)
    implicit val fromFuture = FromFuture.lift[IO]
    implicit val parallel = IO.ioParallel
    implicit val par = Par.liftIO
    implicit val logOf = LogOf[IO](system)

    val result = ActorSystemOf[IO](system).use { implicit system => runF[IO](ec) }
    result.as(ExitCode.Success)
  }


  private def runF[F[_] : Concurrent : ContextShift : Timer : Clock : FromFuture : ToFuture : Par : LogOf](
    blocking: ExecutionContext)(implicit
    system: ActorSystem): F[Unit] = {

    implicit val kafkaConsumerOf = KafkaConsumerOf[F](blocking)

    implicit val kafkaProducerOf = KafkaProducerOf[F](blocking)

    val commonConfig = CommonConfig(bootstrapServers = Nel("localhost:9092"))

    val actorLog = ActorLog(system, ReadEventsApp.getClass)

    implicit val log = Log[F](actorLog)

    val producerConfig = ProducerConfig(common = commonConfig)

    val consumerConfig = ConsumerConfig(common = commonConfig)

    val consumer = Journal.Consumer.of[F](consumerConfig)

    val eventualCassandraConfig = EventualCassandraConfig(
      schema = SchemaConfig(
        keyspace = SchemaConfig.Keyspace(
          name = "keyspace",
          autoCreate = false),
        autoCreate = false),
      client = CassandraConfig(
        contactPoints = Nel("127.0.0.1"),
        authentication = Some(AuthenticationConfig(
          username = "username",
          password = "password"))))

    val journal = for {
      eventualJournal <- EventualCassandra.of[F](eventualCassandraConfig, None)
      headCache       <- HeadCache.of[F](consumerConfig, eventualJournal, None)
      producer        <- Journal.Producer.of[F](producerConfig)
    } yield {
      val journal = Journal[F](None, producer, consumer, eventualJournal, 100.millis, headCache)
      val key = Key(id = "id", topic = "journal")
      for {
        pointer <- journal.pointer(key)
        seqNrs  <- journal.read(key, SeqNr.Min).map(_.seqNr).toList
        _       <- Log[F].info(s"pointer: $pointer")
        _       <- Log[F].info(s"seqNrs: $seqNrs")
      } yield {}
    }

    journal.use(identity)
  }
}
