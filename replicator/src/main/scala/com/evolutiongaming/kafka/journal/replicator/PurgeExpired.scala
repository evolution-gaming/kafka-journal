package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptySet => Nes}
import cats.effect.{Clock, Resource}
import cats.effect.syntax.resource._
import cats.syntax.all._
import cats.{Applicative, Monad, ~>}
import com.evolutiongaming.catshelper.DataHelper._
import com.evolutiongaming.catshelper.{ApplicativeThrowable, FromTry, Log, MeasureDuration, MonadThrowable}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraConsistencyConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraSession, ExpireOn, MetaJournalStatements, SegmentNr}
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.StreamHelper._
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.producer.ProducerConfig
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics._

import scala.concurrent.duration.FiniteDuration

trait PurgeExpired[F[_]] {

  def apply(topic: Topic, expireOn: ExpireOn, segments: Nes[SegmentNr]): F[Long]
}

object PurgeExpired {


  def of[F[_] : MonadThrowable : KafkaProducerOf : CassandraSession : FromTry : Fail : Clock : MeasureDuration : JsonCodec.Encode](
    origin: Option[Origin],
    producerConfig: ProducerConfig,
    tableName: TableName,
    metrics: Option[Metrics[F]],
    consistencyConfig: CassandraConsistencyConfig.Read
  ): Resource[F, PurgeExpired[F]] = {

    implicit val fromAttempt = FromAttempt.lift[F]

    for {
      producer      <- Journals.Producer.of[F](producerConfig)
      selectExpired  = MetaJournalStatements.IdByTopicAndExpireOn.of[F](tableName, consistencyConfig)
      selectExpired <- selectExpired.toResource
    } yield {
      val produce = Produce(producer, origin)
      val purgeExpired = apply(selectExpired, produce)
      metrics
        .fold { purgeExpired } { metrics => purgeExpired.withMetrics(metrics) }
        .enhanceError
    }
  }


  def apply[F[_] : Monad](
    selectExpired: MetaJournalStatements.IdByTopicAndExpireOn[F],
    produce: Produce[F]
  ): PurgeExpired[F] = {

    new PurgeExpired[F] {

      def apply(topic: Topic, expireOn: ExpireOn, segments: Nes[SegmentNr]) = {
        val result = for {
          segment <- segments.toNel.toStream1[F]
          id      <- selectExpired(topic, segment, expireOn)
          key      = Key(id = id, topic = topic)
          result  <- produce.purge(key).toStream
        } yield result
        result.length
      }
    }
  }


  implicit class PurgeExpiredOps[F[_]](val self: PurgeExpired[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): PurgeExpired[G] = new PurgeExpired[G] {

      def apply(topic: Topic, expireOn: ExpireOn, segments: Nes[SegmentNr]) = {
        f(self(topic, expireOn, segments))
      }
    }


    def withLog(
      log: Log[F])(implicit
      F: Monad[F],
      measureDuration: MeasureDuration[F]
    ): PurgeExpired[F] = new PurgeExpired[F] {

      def apply(topic: Topic, expireOn: ExpireOn, segments: Nes[SegmentNr]) = {
        for {
          d <- MeasureDuration[F].start
          r <- self(topic, expireOn, segments)
          d <- d
          _ <- log.debug(s"apply in ${ d.toMillis }ms, topic: $topic, expireOn: $expireOn, segments: ${ segments.mkString_(",") }, result: $r")
        } yield r
      }
    }


    def withMetrics(
      metrics: Metrics[F])(implicit
      F: Monad[F],
      measureDuration: MeasureDuration[F]
    ): PurgeExpired[F] = new PurgeExpired[F] {

      def apply(topic: Topic, expireOn: ExpireOn, segments: Nes[SegmentNr]) = {
        for {
          d <- MeasureDuration[F].start
          r <- self(topic, expireOn, segments)
          d <- d
          _ <- metrics(topic, d, r)
        } yield r
      }
    }


    def enhanceError(implicit F: ApplicativeThrowable[F]): PurgeExpired[F] = {

      def error[A](msg: String, cause: Throwable) = {
        JournalError(s"PurgeExpired.$msg failed with $cause", cause).raiseError[F, A]
      }

      (topic: Topic, expireOn: ExpireOn, segments: Nes[SegmentNr]) => {
        self(topic, expireOn, segments).handleErrorWith { a =>
          error(s"apply topic: $topic, expireOn: $expireOn, segments: ${ segments.mkString_(",") }", a)
        }
      }
    }
  }


  trait Metrics[F[_]] {

    def apply(topic: Topic, latency: FiniteDuration, count: Long): F[Unit]
  }

  object Metrics {

    def empty[F[_] : Applicative]: Metrics[F] = const(().pure[F])

    def const[F[_]](unit: F[Unit]): Metrics[F] = (_: Topic, _: FiniteDuration, _: Long) => unit


    def of[F[_] : Monad](
      registry: CollectorRegistry[F],
      prefix: String = "purge_expired"
    ): Resource[F, Metrics[F]] = {

      val latencySummary = registry.summary(
        name = s"${ prefix }_latency",
        help = "Purge expired latency in seconds",
        quantiles = Quantiles.Default,
        labels = LabelNames("topic"))

      val journalsCounter = registry.summary(
        name = s"${ prefix }_journals",
        help = "Number of expired journals",
        quantiles = Quantiles.Empty,
        labels = LabelNames("topic"))

      for {
        latencySummary  <- latencySummary
        journalsCounter <- journalsCounter
      } yield {

        new Metrics[F] {

          def apply(topic: Topic, latency: FiniteDuration, count: Long) = {
            for {
              _ <- latencySummary.labels(topic).observe(latency.toNanos.nanosToSeconds)
              _ <- journalsCounter.labels(topic).observe(count.toDouble)
            } yield {}
          }
        }
      }
    }
  }
}
