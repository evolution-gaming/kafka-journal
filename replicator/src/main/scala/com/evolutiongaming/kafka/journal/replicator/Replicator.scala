package com.evolutiongaming.kafka.journal.replicator

import cats.data.NonEmptyList as Nel
import cats.effect.syntax.resource.*
import cats.effect.{Ref, *}
import cats.syntax.all.*
import cats.{Applicative, Monad, Parallel, ~>}
import com.evolution.scache.CacheMetrics
import com.evolution.scache.CacheMetrics.Name
import com.evolutiongaming.catshelper.*
import com.evolutiongaming.catshelper.ParallelHelper.*
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.kafka.journal.eventual.ReplicatedJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraCluster, CassandraSession, ReplicatedCassandra}
import com.evolutiongaming.kafka.journal.util.*
import com.evolutiongaming.kafka.journal.util.SkafkaHelper.*
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.{OnError, Retry, Sleep, Strategy}
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerMetrics}
import com.evolutiongaming.skafka.{Bytes as _, ClientId, Topic}
import com.evolutiongaming.smetrics.CollectorRegistry
import scodec.bits.ByteVector

import scala.concurrent.duration.*

/**
 * Subscribes to Kafka and establishes session with Cassandra. For each topic creates instance of
 * [[TopicReplicator]] and binds it with Cassandra client [[ReplicatedCassandra]] (which implements
 * [[ReplicatedJournal]]).
 *
 * [[TopicReplicator]] groups messages per key and delegates them to [[ReplicateRecords]] for
 * storage in Cassandra.
 */
// TODO TEST
trait Replicator[F[_]] {

  def done: F[Boolean]
}

object Replicator {

  def make[
    F[_]: Async: Parallel: FromTry: ToTry: Fail: LogOf: KafkaConsumerOf: FromGFuture: MeasureDuration: JsonCodec,
  ](
    config: ReplicatorConfig,
    cassandraClusterOf: CassandraClusterOf[F],
    hostName: Option[HostName],
    // TODO: [5.0.0 release] replace default value with just None
    // none doesn't compile with Scala 3.3.6
    // None and none[<concrete type from the left>] breaks bincompat
    metrics: Option[Metrics[F]] = none[Nothing],
  ): Resource[F, F[Unit]] = {

    def replicatedJournal(implicit
      cassandraCluster: CassandraCluster[F],
      cassandraSession: CassandraSession[F],
    ) = {
      val origin = hostName.map(Origin.fromHostName)
      ReplicatedCassandra.of[F](config.cassandra, origin, metrics.flatMap(_.journal))
    }

    for {
      cassandraCluster <- CassandraCluster.make(config.cassandra.client, cassandraClusterOf, config.cassandra.retries)
      cassandraSession <- cassandraCluster.session
      replicatedJournal <- replicatedJournal(cassandraCluster, cassandraSession).toResource
      result <- make(config, metrics, replicatedJournal, hostName, ReplicatedOffsetNotifier.empty)
    } yield result
  }

  @deprecated(since = "4.1.7", message = "use 'make' version with added arguments")
  def make[F[_]: Temporal: Parallel: Runtime: FromTry: ToTry: Fail: LogOf: KafkaConsumerOf: MeasureDuration: JsonCodec](
    config: ReplicatorConfig,
    metrics: Option[Metrics[F]],
    journal: ReplicatedJournal[F],
    hostName: Option[HostName],
  ): Resource[F, F[Unit]] = {
    make(
      config = config,
      metrics = metrics,
      journal = journal,
      hostName = hostName,
      replicatedOffsetNotifier = ReplicatedOffsetNotifier.empty,
    )
  }

  def make[F[_]: Temporal: Parallel: Runtime: FromTry: ToTry: Fail: LogOf: KafkaConsumerOf: MeasureDuration: JsonCodec](
    config: ReplicatorConfig,
    metrics: Option[Metrics[F]],
    journal: ReplicatedJournal[F],
    hostName: Option[HostName],
    replicatedOffsetNotifier: ReplicatedOffsetNotifier[F],
  ): Resource[F, F[Unit]] = {

    val topicReplicator: Topic => Resource[F, F[Outcome[F, Throwable, Unit]]] =
      (topic: Topic) => {
        val consumer = TopicReplicator.ConsumerOf.make[F](topic, config.kafka.consumer, config.pollTimeout, hostName)

        val metrics1 = metrics
          .flatMap { _.replicator }
          .fold { TopicReplicatorMetrics.empty[F] } { metrics => metrics(topic) }

        val cacheOf = CacheOf[F](config.cacheExpireAfter, metrics.flatMap(_.cache))
        TopicReplicator.make(topic, journal, consumer, metrics1, cacheOf, replicatedOffsetNotifier)
      }

    val consumer = Consumer.make[F](config.kafka.consumer)

    make[F](config = Config(config), consumer = consumer, topicReplicatorOf = topicReplicator)
  }

  def make[F[_]: Concurrent: Sleep: Parallel: LogOf: MeasureDuration](
    config: Config,
    consumer: Resource[F, Consumer[F]],
    topicReplicatorOf: Topic => Resource[F, F[Outcome[F, Throwable, Unit]]],
  ): Resource[F, F[Unit]] = {

    def retry(log: Log[F]) = for {
      random <- Random.State.fromClock[F]()
    } yield {
      val strategy = Strategy
        .exponential(100.millis)
        .jitter(random)
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
      registry <- ResourceRegistry.make[F]
    } yield {
      for {
        log <- LogOf[F].apply(Replicator.getClass)
        retry <- retry(log)
        error <- Ref.of[F, F[Unit]](().pure[F])
        result <- {
          val topicReplicator = topicReplicatorOf.andThen { topicReplicator =>
            // TODO
            registry.allocate {
              val fiber = for {
                fiber <- StartResource(topicReplicator) { (outcomeF: F[Outcome[F, Throwable, Unit]]) =>
                  outcomeF
                    .flatTap {
                      case Outcome.Errored(e) => error.set(e.raiseError[F, Unit])
                      case _ => Concurrent[F].unit
                    }
                    .onError { case e => error.set(e.raiseError[F, Unit]) }
                }
              } yield {
                ((), fiber.cancel)
              }
              Resource(fiber)
            }
          }

          val consumer1 = consumer.mapMethod(retry)

          start(config, consumer1, topicReplicator, error.get.flatten, log)
        }
      } yield result
    }
  }

  def start[F[_]: Concurrent: Sleep: Parallel: MeasureDuration](
    config: Config,
    consumer: Consumer[F],
    start: Topic => F[Unit],
    continue: F[Unit],
    log: Log[F],
  ): F[Unit] = {

    type State = Set[Topic]

    def newTopics(state: State) = {
      for {
        latency <- MeasureDuration[F].start
        topics <- consumer.topics
        latency <- latency
        topicsNew = for {
          topic <- (topics -- state).toList
          if config.topicPrefixes exists topic.startsWith
        } yield topic
        _ <- {
          if (topicsNew.isEmpty) ().pure[F]
          else
            log.info {
              val topics = topicsNew.mkString(",")
              s"discovered new topics in ${ latency.toMillis }ms: $topics"
            }
        }
      } yield topicsNew
    }

    val sleep = Sleep[F].sleep(config.topicDiscoveryInterval)

    Set
      .empty[Topic]
      .tailRecM { state =>
        for {
          topics <- newTopics(state)
          _ <- continue
          _ <- topics.parFoldMap1(start)
          _ <- continue
          _ <- sleep
          _ <- continue
        } yield {
          (state ++ topics).asLeft[Unit]
        }
      }
      .onError { case e => log.error(s"failed with $e", e) }
  }

  final case class Config(
    topicPrefixes: Nel[String] = Nel.of("journal"),
    topicDiscoveryInterval: FiniteDuration = 3.seconds,
  )

  object Config {
    val default: Config = Config()

    def apply(config: ReplicatorConfig): Config = {
      Config(topicPrefixes = config.topicPrefixes, topicDiscoveryInterval = config.topicDiscoveryInterval)
    }
  }

  trait Consumer[F[_]] {
    def topics: F[Set[Topic]]
  }

  object Consumer {

    def apply[F[_]](
      implicit
      F: Consumer[F],
    ): Consumer[F] = F

    def apply[F[_]](consumer: KafkaConsumer[F, String, ByteVector]): Consumer[F] = new Consumer[F] {
      def topics = consumer.topics
    }

    def make[F[_]: Applicative: KafkaConsumerOf: FromTry](config: ConsumerConfig): Resource[F, Consumer[F]] = {
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

    def journal: Option[ReplicatedJournal.Metrics[F]]

    def replicator: Option[Topic => TopicReplicatorMetrics[F]]

    def consumer: Option[ConsumerMetrics[F]]

    def cache: Option[CacheMetrics.Name => CacheMetrics[F]]
  }

  object Metrics {

    def empty[F[_]]: Metrics[F] = new Metrics[F] {

      def journal: Option[ReplicatedJournal.Metrics[F]] = none

      def replicator: Option[Topic => TopicReplicatorMetrics[F]] = none

      def consumer: Option[ConsumerMetrics[F]] = none

      def cache: Option[Name => CacheMetrics[F]] = none
    }

    def make[F[_]: Monad](registry: CollectorRegistry[F], clientId: ClientId): Resource[F, Replicator.Metrics[F]] = {
      for {
        replicator1 <- TopicReplicatorMetrics.make[F](registry)
        journal1 <- ReplicatedJournal.Metrics.make[F](registry)
        consumer1 <- ConsumerMetrics.of[F](registry)
        cache1 <- CacheMetrics.of[F](registry)
      } yield {
        new Metrics[F] {

          val journal = journal1.some

          val replicator = replicator1.some

          val consumer = consumer1(clientId).some

          val cache = cache1.some
        }
      }
    }
  }
}
