package com.evolutiongaming.kafka.journal.replicator


import cats.data.{NonEmptyList => Nel}
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import cats.{Applicative, Monad, Parallel, ~>}
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.catshelper._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.ReplicatedJournalOld
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraCluster, CassandraSession, ReplicatedCassandra}
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.kafka.journal.util._
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.{OnError, Retry, Strategy}
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{ClientId, Topic, Bytes => _}
import com.evolutiongaming.smetrics.{CollectorRegistry, MeasureDuration}
import scodec.bits.ByteVector

import scala.concurrent.duration._

// TODO TEST
trait Replicator[F[_]] {

  def done: F[Boolean]
}

object Replicator {

  def of[F[_] : Concurrent : Timer : Parallel : FromFuture : ToFuture : LogOf : KafkaConsumerOf : FromGFuture : MeasureDuration : FromTry](
    config: ReplicatorConfig,
    cassandraClusterOf: CassandraClusterOf[F],
    hostName: Option[HostName],
    metrics: Option[Metrics[F]] = none
  ): Resource[F, F[Unit]] = {

    def replicatedJournal(implicit
      cassandraCluster: CassandraCluster[F],
      cassandraSession: CassandraSession[F]
    ) = {
      val origin = hostName.map(Origin.fromHostName)
      ReplicatedCassandra.of[F](config.cassandra, origin, metrics.flatMap(_.journal))
    }

    for {
      cassandraCluster  <- CassandraCluster.of(config.cassandra.client, cassandraClusterOf, config.cassandra.retries)
      cassandraSession  <- cassandraCluster.session
      replicatedJournal <- Resource.liftF(replicatedJournal(cassandraCluster, cassandraSession))
      result            <- of(config, metrics, replicatedJournal, hostName)
    } yield result
  }

  def of[F[_] : Concurrent : Timer : Parallel : LogOf : KafkaConsumerOf : MeasureDuration : FromTry](
    config: ReplicatorConfig,
    metrics: Option[Metrics[F]],
    journal: ReplicatedJournalOld[F],
    hostName: Option[HostName]
  ): Resource[F, F[Unit]] = {

    val topicReplicator = (topic: Topic) => {

      val consumer = TopicReplicator.ConsumerOf.of[F](
        topic,
        config.consumer,
        config.pollTimeout,
        hostName)

      val metrics1 = metrics
        .flatMap(_.replicator)
        .fold { TopicReplicator.Metrics.empty[F] } { metrics => metrics(topic) }

      TopicReplicator.of(topic, journal, consumer, metrics1)
    }

    val consumer = Consumer.of[F](config.consumer)

    of(Config(config), consumer, topicReplicator)
  }

  def of[F[_] : Concurrent : Timer : Parallel : LogOf : MeasureDuration](
    config: Config,
    consumer: Resource[F, Consumer[F]],
    topicReplicatorOf: Topic => Resource[F, F[Unit]]
  ): Resource[F, F[Unit]] = {

    def retry(log: Log[F]) = for {
      random <- Random.State.fromClock[F]()
    } yield {
      val strategy = Strategy
        .fullJitter(100.millis, random)
        .limit(1.minute)
      new Named[F] {
        def apply[A](fa: F[A], name: String) = {
          val onError = OnError.fromLog(log.prefixed(s"consumer.$name"))
          val retry = Retry(strategy, onError)
          retry(fa)
        }
      }
    }

      for {
      consumer <- consumer
      registry <- ResourceRegistry.of[F]
    } yield {
      for {
        log    <- LogOf[F].apply(Replicator.getClass)
        retry  <- retry(log)
        error  <- Ref.of[F, F[Unit]](().pure[F])
        result <- {
          val topicReplicator = topicReplicatorOf.andThen { topicReplicator =>
            // TODO
            registry.allocate {
              val fiber = for {
                fiber <- topicReplicator.start { _.onError { case e => error.set(e.raiseError[F, Unit]) } }
              } yield {
                ((), fiber.cancel)
              }
              Resource(fiber)
            }
          }

          val consumer1 = consumer.mapMethod(retry)

          implicit val log1 = log
          start(config, consumer1, topicReplicator, error.get.flatten)
        }
      } yield result
    }
  }


  def start[F[_] : Sync : Parallel : Timer : Log : MeasureDuration](
    config: Config,
    consumer: Consumer[F],
    start: Topic => F[Unit],
    continue: F[Unit]
  ): F[Unit] = {

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
            s"discovered new topics in ${ latency.toMillis }ms: $topics"
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
    topicPrefixes: Nel[String] = Nel.of("journal"),
    topicDiscoveryInterval: FiniteDuration = 3.seconds)

  object Config {
    val default: Config = Config()

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

    def apply[F[_]](consumer: KafkaConsumer[F, String, ByteVector]): Consumer[F] = new Consumer[F] {
      def topics = consumer.topics
    }


    def of[F[_] : Applicative : KafkaConsumerOf : FromTry](config: ConsumerConfig): Resource[F, Consumer[F]] = {
      for {
        consumer <- KafkaConsumerOf[F].apply[String, ByteVector](config)
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


  trait Metrics[F[_]] {

    def journal: Option[ReplicatedJournalOld.Metrics[F]]

    def replicator: Option[Topic => TopicReplicator.Metrics[F]]

    def consumer: Option[ConsumerMetrics[F]]
  }

  object Metrics {

    def empty[F[_]]: Metrics[F] = new Metrics[F] {

      def journal = None

      def replicator = None

      def consumer = None
    }


    def of[F[_] : Monad](registry: CollectorRegistry[F], clientId: ClientId): Resource[F, Replicator.Metrics[F]] = {
      for {
        replicator1 <- TopicReplicator.Metrics.of[F](registry)
        journal1    <- ReplicatedJournalOld.Metrics.of[F](registry)
        consumer1   <- ConsumerMetrics.of[F](registry)
      } yield {
        new Metrics[F] {

          val journal = journal1.some

          val replicator = replicator1.some

          val consumer = consumer1(clientId).some
        }
      }
    }
  }
}
