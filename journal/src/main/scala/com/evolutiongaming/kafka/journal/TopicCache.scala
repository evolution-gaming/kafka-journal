package com.evolutiongaming.kafka.journal

import cats._
import cats.data.{NonEmptyMap => Nem, NonEmptySet => Nes}
import cats.effect._
import cats.effect.syntax.all._
import cats.syntax.all._
import com.evolutiongaming.catshelper._
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.kafka.journal.conversions.ConsRecordToActionHeader
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.kafka.journal.HeadCache.Eventual
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.Retry.implicits._
import com.evolutiongaming.retry.{Sleep, Strategy}
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, ConsumerRecords}
import com.evolutiongaming.skafka.{Offset, Partition, Topic, TopicPartition}
import com.evolution.scache.Cache

import scala.concurrent.duration._


trait TopicCache[F[_]] {

  def get(id: String, partition: Partition, offset: Offset): F[PartitionCache.Result[F]]
}

object TopicCache {

  def of[F[_]: Async: Parallel: Runtime](
    eventual: Eventual[F],
    topic: Topic,
    log: Log[F],
    consumer: Resource[F, Consumer[F]],
    config: HeadCacheConfig,
    consRecordToActionHeader: ConsRecordToActionHeader[F],
    metrics: Option[HeadCache.Metrics[F]]
  ): Resource[F, TopicCache[F]] = {

    for {
      consumer <- consumer
        .map { _.withLog(log) }
        .pure[Resource[F, *]]
      partitions <- consumer
        .use { consumer =>
          consumer.partitions(topic)
        }
        .toResource
      cache <- Cache.loading[F, Partition, PartitionCache[F]]
      partitionCacheOf = (partition: Partition) => {
        cache.getOrUpdateResource(partition) {
          PartitionCache.of(
            maxSize = config.partition.maxSize,
            dropUponLimit = config.partition.dropUponLimit,
            timeout = config.timeout)
        }
      }
      remove =
        partitions
          .foldMapM { partition =>
            for {
              offset <- eventual.pointer(topic, partition)
              cache <- partitionCacheOf(partition)
              diff <- offset.flatTraverse(cache.remove)
            } yield diff match {
              case Some(a) => Sample(a.value)
              case None    => Sample.Empty
            }
          }
          .flatMap { sample =>
            sample.avg
              .foldMapM { diff =>
                metrics.foldMapM { _.storage(topic, diff) }
              }
          }
      _ <- remove.toResource
      pointers = {
        cache
          .values1
          .flatMap { values =>
            values
              .toList
              .traverseFilter { case (partition, cache) =>
                cache
                  .toOption
                  .traverse { cache =>
                    cache
                      .offset
                      .flatMap {
                        case Some(offset) => offset.inc[F]
                        case None         => Offset.min.pure[F]
                      }
                      .map { offset => (partition, offset) }
                  }
              }
          }
          .map { _.toMap }
      }
      _ <- HeadCacheConsumption
        .apply(
          topic = topic,
          pointers = pointers,
          consumer = consumer,
          log = log)
        .foreach { records =>
          for {
            now <- Clock[F].realTime
            result <- records
              .values
              .parFoldMap1 { case (topicPartition, records) =>
                records
                  .traverse { record =>
                    record
                      .key
                      .traverseFilter { key =>
                        consRecordToActionHeader
                          .apply(record)
                          .map { header => PartitionCache.Record.Data(key.value, header) }
                          .value
                      }
                      .map { data =>
                        PartitionCache.Record(record.offset, data)
                      }
                  }
                  .flatMap { records =>
                    partitionCacheOf
                      .apply(topicPartition.partition)
                      .flatMap { cache =>
                        cache
                          .add(records)
                          .map {
                            case Some(a) => Sample(a.value)
                            case None    => Sample.Empty
                          }
                      }
                  }
              }
              .flatMap { sample =>
                metrics.foldMapM { metrics =>
                  sample
                    .avg
                    .foldMapM { diff =>
                      records
                        .values
                        .foldLeft(none[Long]) { case (timestamp, (_, records)) =>
                          records
                            .foldLeft(timestamp) { case (timestamp, record) =>
                              record
                                .timestampAndType
                                .fold {
                                  timestamp
                                } { timestampAndType =>
                                  val timestamp1 = timestampAndType
                                    .timestamp
                                    .toEpochMilli
                                  timestamp
                                    .fold { timestamp1 } { _.min(timestamp1) }
                                    .some
                                }
                            }
                        }
                        .foldMapM { timestamp =>
                          metrics.consumer(topic, age = now - timestamp.millis, diff = diff)
                        }
                    }
                }
              }
          } yield result
        }
        .onError { case a => log.error(s"consuming failed with $a", a) } /*TODO headcache: fail head cache*/
        .background
      random <- Random.State.fromClock[F]().toResource
      strategy = Strategy
        .exponential(10.millis)
        .cap(3.seconds)
        .jitter(random)
      _ <- Sleep[F]
        .sleep(config.removeInterval)
        .productR { remove }
        .retry(strategy)
        .handleErrorWith { a => log.error(s"remove failed, error: $a", a) }
        .foreverM[Unit]
        .background
      _ <- metrics.foldMapM { metrics =>
        val result = for {
          _ <- Temporal[F].sleep(1.minute)
          a <- cache.foldMap { case (_, value) => value.foldMapM { _.meters } }
          a <- metrics.meters(topic, entries = a.entries, listeners = a.listeners)
        } yield a
        result
          .handleErrorWith { a => log.error(s"metrics.listeners failed, error: $a", a) }
          .foreverM[Unit]
          .background
          .void
      }
    } yield {
      class Main
      new Main with TopicCache[F] {

        def get(id: String, partition: Partition, offset: Offset) = {
          partitionCacheOf(partition).flatMap { _.get(id, offset) }
        }
      }
    }
  }

  trait Consumer[F[_]] {

    def assign(topic: Topic, partitions: Nes[Partition]): F[Unit]

    def seek(topic: Topic, offsets: Nem[Partition, Offset]): F[Unit]

    def poll: F[ConsumerRecords[String, Unit]]

    def partitions(topic: Topic): F[Set[Partition]]
  }

  object Consumer {

    def empty[F[_]: Applicative]: Consumer[F] = {
      class Empty
      new Empty with Consumer[F] {

        def assign(topic: Topic, partitions: Nes[Partition]) = ().pure[F]

        def seek(topic: Topic, offsets: Nem[Partition, Offset]) = ().pure[F]

        def poll = ConsumerRecords.empty[String, Unit].pure[F]

        def partitions(topic: Topic) = Set.empty[Partition].pure[F]
      }
    }


    def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

    def apply[F[_]: Monad](
      consumer: KafkaConsumer[F, String, Unit],
      pollTimeout: FiniteDuration
    ): Consumer[F] = {

      class Main
      new Main with Consumer[F] {

        def assign(topic: Topic, partitions: Nes[Partition]) = {
          val partitions1 = partitions.map { partition =>
            TopicPartition(topic = topic, partition)
          }
          consumer.assign(partitions1)
        }

        def seek(topic: Topic, offsets: Nem[Partition, Offset]) = {
          offsets.toNel.foldMapM { case (partition, offset) =>
            val topicPartition = TopicPartition(topic = topic, partition = partition)
            consumer.seek(topicPartition, offset)
          }
        }

        val poll = consumer.poll(pollTimeout)

        def partitions(topic: Topic) = consumer.partitions(topic)
      }
    }

    def of[F[_]: Monad: KafkaConsumerOf: FromTry](
      config: ConsumerConfig,
      pollTimeout: FiniteDuration = 10.millis
    ): Resource[F, Consumer[F]] = {
      val config1 = config.copy(
        autoOffsetReset = AutoOffsetReset.Earliest,
        groupId = None,
        autoCommit = false)
      for {
        consumer <- KafkaConsumerOf[F].apply[String, Unit](config1)
      } yield {
        Consumer[F](consumer, pollTimeout)
      }
    }

    private abstract sealed class WithLog

    implicit class ConsumerOps[F[_]](val self: Consumer[F]) extends AnyVal {

      def withLog(log: Log[F])(implicit F: Monad[F]): Consumer[F] = {
        new WithLog with Consumer[F] {

          def assign(topic: Topic, partitions: Nes[Partition]) = {
            for {
              _ <- log.debug(s"assign topic: $topic, partitions: $partitions")
              a <- self.assign(topic, partitions)
            } yield a
          }

          def seek(topic: Topic, offsets: Nem[Partition, Offset]) = {
            for {
              _ <- log.debug(s"seek topic: $topic, offsets: $offsets")
              a <- self.seek(topic, offsets)
            } yield a
          }

          def poll = {
            for {
              a <- self.poll
              _ <- {
                if (a.values.isEmpty) {
                  ().pure[F]
                } else {
                  log.debug {
                    val size = a.values.values.foldLeft(0L) { _ + _.size }
                    s"poll result: $size"
                  }
                }
              }
            } yield a
          }

          def partitions(topic: Topic) = {
            for {
              a <- self.partitions(topic)
              _ <- log.debug(s"partitions topic: $topic, result: $a")
            } yield a
          }
        }
      }
    }
  }


  private final case class Sample(sum: Long, count: Int)

  private object Sample {

    def apply(value: Long): Sample = Sample(sum = value, count = 1)

    val Empty: Sample = Sample(0L, 0)

    implicit val monoidSample: Monoid[Sample] = new Monoid[Sample] {

      def empty = Empty

      def combine(a: Sample, b: Sample) = {
        Sample(
          sum = a.sum.combine(b.sum),
          count = a.count.combine(b.count))
      }
    }

    implicit class SampleOps(val self: Sample) extends AnyVal {
      def avg: Option[Long] = {
        if (self.count > 0) (self.sum / self.count).some else none
      }
    }
  }

  private sealed abstract class WithMetrics

  private sealed abstract class WithLog

  implicit class TopicCacheOps[F[_]](val self: TopicCache[F]) extends AnyVal {

    def withMetrics(
      topic: Topic,
      metrics: HeadCache.Metrics[F])(implicit
      F: MonadThrowable[F],
      measureDuration: MeasureDuration[F]
    ): TopicCache[F] = {
      new WithMetrics with TopicCache[F] {

        def get(id: String, partition: Partition, offset: Offset) = {
          import com.evolutiongaming.kafka.journal.PartitionCache.Result
          for {
            d <- MeasureDuration[F].start
            f = (result: Either[Throwable, Result.Now], hit: Boolean) => {
              val name = result match {
                case Right(_: Result.Now.Value)   => "value"
                case Right(Result.Now.Ahead)      => "ahead"
                case Right(Result.Now.Limited)    => "limited"
                case Right(_: Result.Now.Timeout) => "timeout"
                case Left(_)                      => "failure"
              }
              for {
                d <- d
                _ <- metrics.get(topic, d, name, hit)
                a <- result.liftTo[F]
              } yield a
            }
            a <- self
              .get(id, partition, offset)
              .attempt
            a <- a match {
              case Right(a: Result.Now) =>
                f(a.asRight, true)

              case Right(Result.Later.Behind(a)) =>
                val result = a
                  .attempt
                  .flatMap { a => f(a, false) }
                Result
                  .behind(result)
                  .pure[F]

              case Right(Result.Later.Empty(a)) =>
                val result = a
                  .attempt
                  .flatMap { a => f(a, false) }
                Result
                  .empty(result)
                  .pure[F]

              case Left(a) =>
                f(a.asLeft, true)
            }
          } yield a
        }
      }
    }

    def withLog(log: Log[F])(implicit F: FlatMap[F], measureDuration: MeasureDuration[F]): TopicCache[F] = {
      new WithLog with TopicCache[F] {

        def get(id: String, partition: Partition, offset: Offset) = {
          for {
            d <- MeasureDuration[F].start
            a <- self.get(id, partition, offset)
            d <- d
            _ <- log.debug(s"get in ${ d.toMillis }ms, id: $id, offset: $partition:$offset, result: $a")
          } yield a
        }
      }
    }
  }

  private implicit class SetOps[A](val self: Set[A]) extends AnyVal {

    def foldMapM[F[_]: Monad, B: Monoid](f: A => F[B]): F[B] = {
      self.foldLeft(Monoid[B].empty.pure[F]) { case (b0, a) =>
        for {
          b0 <- b0
          b1 <- f(a)
        } yield b0.combine(b1)
      }
    }

  }

}
