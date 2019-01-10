package com.evolutiongaming.kafka.journal

import akka.actor.ActorSystem
import cats.effect._
import cats.implicits._
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal.eventual.cassandra._
import com.evolutiongaming.kafka.journal.util.{FromFuture, Par, ToFuture}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.scassandra.{AuthenticationConfig, CassandraConfig}
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.producer.ProducerConfig

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object ReadEventsApp extends IOApp {

  def run(args: List[String]) = {
    implicit val system = ActorSystem("ReadEventsApp")
    implicit val ec = system.dispatcher
    implicit val timer = IO.timer(ec)
    implicit val fromFuture = FromFuture.lift[IO]
    implicit val parallel = IO.ioParallel
    implicit val par = Par.lift
    implicit val logOf = LogOf[IO](system)

    val terminate = FromFuture[IO].apply { system.terminate() }.void
    runF[IO](ec)
      .guarantee(terminate)
      .as(ExitCode.Success)
  }


  private def runF[F[_] : Concurrent : ContextShift : Timer : Clock : FromFuture : ToFuture : Par : LogOf](
    blocking: ExecutionContext)(implicit
    system: ActorSystem): F[Unit] = {

    val commonConfig = CommonConfig(bootstrapServers = Nel("localhost:9092"))

    val actorLog = ActorLog(system, ReadEventsApp.getClass)

    implicit val log = Log[F](actorLog)

    val producerConfig = ProducerConfig(common = commonConfig)

    val consumerConfig = ConsumerConfig(common = commonConfig)

    val topicConsumer = TopicConsumer[F](consumerConfig, blocking)

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

    def eventualJournal(implicit cassandraSession: CassandraSession[F]) = {
      EventualCassandra.of[F](eventualCassandraConfig, None)
    }

    val journal = for {
      cassandraCluster <- CassandraCluster.of[F](eventualCassandraConfig.client, eventualCassandraConfig.retries)
      cassandraSession <- cassandraCluster.session
      eventualJournal  <- Resource.liftF(eventualJournal(cassandraSession))
      headCache        <- Resource.liftF(HeadCache.of[F](consumerConfig, eventualJournal, blocking))
      kafkaProducer    <- KafkaProducer.of[F](producerConfig, blocking)
    } yield {
      val journal = Journal[F](None, kafkaProducer, topicConsumer, eventualJournal, 100.millis, headCache)
      val key = Key(id = "id", topic = "journal")
      for {
        pointer <- journal.pointer(key)
        seqNrs <- journal.read(key, SeqNr.Min, List.empty[SeqNr]) { case (result, event) => (event.seqNr :: result).continue }
        _ <- Log[F].info(s"pointer: $pointer")
        _ <- Log[F].info(s"seqNrs: $seqNrs")
      } yield {}
    }

    journal.use(identity)
  }
}
