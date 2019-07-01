package com.evolutiongaming.kafka.journal.replicator


import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import cats.temp.par._
import cats.~>
import com.evolutiongaming.catshelper.{FromFuture, Log, LogOf, ToFuture}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.ReplicatedJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraCluster, CassandraSession, ReplicatedCassandra}
import com.evolutiongaming.retry.Retry
import com.evolutiongaming.kafka.journal.CatsHelper._
import com.evolutiongaming.random.Random
import com.evolutiongaming.kafka.journal.util._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{Topic, Bytes => _}
import com.evolutiongaming.smetrics.MeasureDuration

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

// TODO TEST
trait Replicator[F[_]] {

  def done: F[Boolean]
}

object Replicator {

  def of[F[_] : Concurrent : Timer : Par : FromFuture : ToFuture : ContextShift : LogOf : KafkaConsumerOf : FromGFuture : MeasureDuration](
    config: ReplicatorConfig,
    executor: ExecutionContextExecutor,
    hostName: Option[HostName] = HostName(),
    metrics: Option[Metrics[F]] = none
  ): Resource[F, F[Unit]] = {

    implicit val clock = Timer[F].clock

    def replicatedJournal(implicit cassandraCluster: CassandraCluster[F], cassandraSession: CassandraSession[F]) = {
      ReplicatedCassandra.of[F](config.cassandra, metrics.flatMap(_.journal))
    }

    for {
      cassandraCluster  <- CassandraCluster.of(config.cassandra.client, config.cassandra.retries, executor)
      cassandraSession  <- cassandraCluster.session
      replicatedJournal <- Resource.liftF(replicatedJournal(cassandraCluster, cassandraSession))
      result            <- of(config, metrics, replicatedJournal, hostName)
    } yield result
  }

  def of[F[_] : Concurrent : Timer : Par : ContextShift : LogOf : KafkaConsumerOf : MeasureDuration](
    config: ReplicatorConfig,
    metrics: Option[Metrics[F]]/*TODO not used for kafka*/,
    replicatedJournal: ReplicatedJournal[F],
    hostName: Option[HostName]): Resource[F, F[Unit]] = {

    implicit val replicatedJournal1 = replicatedJournal

    val topicReplicator = (topic: Topic) => {

      val consumer = TopicReplicator.Consumer.of[F](topic, config.consumer, config.pollTimeout, hostName)

      implicit val metrics1 = metrics
        .flatMap(_.replicator)
        .fold(TopicReplicator.Metrics.empty[F])(_.apply(topic))

      val result = for {
        topicReplicator <- TopicReplicator.of[F](topic = topic, consumer = consumer)
      } yield {
        (topicReplicator.done, topicReplicator.close)
      }
      Resource(result)
    }

    val consumer = Consumer.of[F](config.consumer)

    of(Config(config), consumer, topicReplicator)
  }

  def of[F[_] : Concurrent : Timer : Par : ContextShift : LogOf : MeasureDuration](
    config: Config,
    consumer: Resource[F, Consumer[F]],
    topicReplicatorOf: Topic => Resource[F, F[Unit]]): Resource[F, F[Unit]] = {

    for {
      consumer <- consumer
      registry <- ResourceRegistry.of[F]
    } yield {
      for {
        log    <- LogOf[F].apply(Replicator.getClass)
        random <- Random.State.fromClock[F]()
        error  <- Ref.of[F, F[Unit]](().pure[F])
        result <- {
          val topicReplicator = topicReplicatorOf.andThen { topicReplicator =>
            registry.allocate {
              val fiber = for {
                fiber <- topicReplicator.start { _.onError { case e => error.set(e.raiseError[F, Unit]) } }
              } yield {
                ((), fiber.cancel)
              }
              Resource(fiber)
            }
          }

          val strategy = Retry.Strategy.fullJitter(100.millis, random).limit(1.minute)

          def onError(name: String)(error: Throwable, details: Retry.Details) = {
            details.decision match {
              case Retry.Decision.Retry(delay) =>
                log.warn(s"$name failed, retrying in $delay, error: $error")

              case Retry.Decision.GiveUp =>
                log.error(s"$name failed after ${ details.retries } retries, error: $error", error)
            }
          }

          val retry = new Named[F] {
            def apply[A](fa: F[A], name: String) = Retry(strategy, onError(s"consumer.$name")).apply(fa)
          }

          val consumerRetry = consumer.mapMethod(retry)

          implicit val log1 = log
          start(config, consumerRetry, topicReplicator, error.get.flatten)
        }
      } yield result
    }
  }


  def start[F[_] : Sync : Par : Timer : Log : MeasureDuration](
    config: Config,
    consumer: Consumer[F],
    start: Topic => F[Unit],
    continue: F[Unit]): F[Unit] = {

    type State = Set[Topic]

    def newTopics(state: State) = {
      for {
        latency <- MeasureDuration[F].start
        topics  <- consumer.topics
        latency <- latency
        topicsNew = for {
          topic <- (topics -- state).toList
          if config.topicPrefixes exists topic.startsWith
        } yield topic
        _ <- {
          if (topicsNew.isEmpty) ().pure[F]
          else Log[F].info {
            val topics = topicsNew.mkString(",")
            s"discovered new topics in ${ latency }ms: $topics"
          }
        }
      } yield topicsNew
    }

    val sleep = Timer[F].sleep(config.topicDiscoveryInterval)

    def loop(state: State): F[State] = {
      val result = for {
        topics <- newTopics(state)
        _      <- continue
        _      <- topics.parFoldMap(start)
        _      <- continue
        _      <- sleep
        _      <- continue
      } yield state ++ topics
      result >>= loop
    }

    loop(Set.empty).void.onError { case e => Log[F].error(s"failed with $e", e) }
  }


  final case class Config(
    topicPrefixes: Nel[String] = Nel("journal"),
    topicDiscoveryInterval: FiniteDuration = 3.seconds)

  object Config {
    val Default: Config = Config()

    def apply(config: ReplicatorConfig): Config = {
      Config(
        topicPrefixes = config.topicPrefixes,
        topicDiscoveryInterval = config.topicDiscoveryInterval)
    }
  }


  trait Consumer[F[_]] {
    def topics: F[Set[Topic]]
  }

  object Consumer {

    def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

    def apply[F[_]](consumer: KafkaConsumer[F, Id, Bytes]): Consumer[F] = new Consumer[F] {
      def topics = consumer.topics
    }


    def of[F[_] : Sync : KafkaConsumerOf](config: ConsumerConfig): Resource[F, Consumer[F]] = {
      for {
        consumer <- KafkaConsumerOf[F].apply[Id, Bytes](config)
      } yield {
        Consumer[F](consumer)
      }
    }


    implicit class ConsumerOps[F[_]](val self: Consumer[F]) extends AnyVal {

      def mapK[G[_]](f: F ~> G): Consumer[G] = new Consumer[G] {
        def topics = f(self.topics)
      }

      def mapMethod(f: Named[F]): Consumer[F] = new Consumer[F] {
        def topics = f(self.topics, "topics")
      }
    }
  }


  final case class Metrics[F[_]](
    journal: Option[ReplicatedJournal.Metrics[F]] = None,
    replicator: Option[Topic => TopicReplicator.Metrics[F]] = None,
    consumer: Option[ConsumerMetrics[F]] = None)

  object Metrics {
    def empty[F[_]]: Metrics[F] = Metrics()
  }
}
