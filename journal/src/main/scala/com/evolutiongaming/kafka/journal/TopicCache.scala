package com.evolutiongaming.kafka.journal

import cats.*
import cats.data.{NonEmptyMap as Nem, NonEmptySet as Nes}
import cats.effect.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.evolutiongaming.catshelper.*
import com.evolutiongaming.catshelper.ParallelHelper.*
import com.evolutiongaming.kafka.journal.HeadCache.Eventual
import com.evolutiongaming.kafka.journal.conversions.ConsRecordToActionHeader
import com.evolutiongaming.kafka.journal.util.SkafkaHelper.*
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.Retry.implicits.*
import com.evolutiongaming.retry.{Sleep, Strategy}
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, ConsumerRecords}
import com.evolutiongaming.skafka.{Offset, Partition, Topic, TopicPartition}

import scala.concurrent.duration.*

/** Maintains an information about non-replicated Kafka records in a topic.
  *
  * The implementation reads both Kafka and Cassandra by itself, continously
  * refreshing the information.
  */
private[journal] trait TopicCache[F[_]] {

  /** Get the information about a state of a journal stored in the topic.
    *
    * @param id
    *   Journal id
    * @param partition
    *   Partition where journal is stored to. The usual way to get the partition
    *   is to write a "marker" record to Kafka topic and use the partition of
    *   the marker as a current one.
    * @param offset
    *   Current [[Offset]], i.e. maximum offset where Kafka records related to a
    *   journal are located. The usual way to get such an offset is to write a
    *   "marker" record to Kafka patition and use the offset of the marker as a
    *   current one.
    *
    * @return
    *   [[PartitionCache.Result]] with either the current state or indication of
    *   a reason why such state is not present in a cache.
    *
    * @see
    *   [[PartitionCache.Result]] for more details on possible results.
    */
  def get(id: String, partition: Partition, offset: Offset): F[PartitionCache.Result[F]]
}

private[journal] object TopicCache {

  /** Creates [[TopicCache]] using configured parameters and data sources.
   *
   * @param eventual
   *   Cassandra data source.
   * @param topic
   *   Topic stored in this cache.
   * @param log
   *   Logger used to write debug logs to.
   * @param consumer
   *   Kafka data source factory. The reason why it is factory (i.e.
   *   `Resource`) is that [[HeadCache]] will try to recreate consumer in case
   *   of the failure.
   * @param config
   *   [[HeadCache]] configuration.
   * @param consRecordToActionHeader
   *   Function used to parse records coming from `consumer`. Only headers will
   *   be parsed, and the payload will be ignored.
   * @param metrics
   *   Interface to report the metrics to.
   * @return
   *   Resource which will configure a [[TopicCache]] with the passed
   *   parameters. Instance of `Resource[TopicCache]` are, obviously, reusable
   *   and there is no need to call [[TopicCache#of]] each time if parameters
   *   did not change.
   */
  def make[F[_]: Async: Parallel](
    eventual: Eventual[F],
    topic: Topic,
    log: Log[F],
    consumer: Resource[F, Consumer[F]],
    config: HeadCacheConfig,
    consRecordToActionHeader: ConsRecordToActionHeader[F],
    metrics: Option[HeadCache.Metrics[F]],
  ): Resource[F, TopicCache[F]] = {

    for {
      consumer <- consumer
        .map { _.withLog(log) }
        .pure[Resource[F, *]]
      partitions <- consumer.use { _.partitions(topic) }.toResource

      caches <- partitions
        .toList
        .parTraverse { partition =>
          PartitionCache
            .make(maxSize = config.partition.maxSize, dropUponLimit = config.partition.dropUponLimit, timeout = config.timeout)
            .map { partitionCache =>
              (partition, partitionCache)
            }
        }

      cachesMap = caches.toMap

      remove = caches
        .foldMapM {
          case (partition, cache) =>
            for {
              offset <- eventual.pointer(topic, partition)
              result <- offset.foldMapM { offset =>
                cache
                  .remove(offset)
                  .map { diff =>
                    diff.foldMap { a => Sample(a.value) }
                  }
              }
            } yield result
        }
        .flatMap { sample =>
          sample
            .avg
            .foldMapM { diff =>
              metrics.foldMapM { _.storage(topic, diff) }
            }
        }
      _ <- remove.toResource
      pointers = caches
        .traverse {
          case (partition, cache) =>
            cache
              .offset
              .flatMap {
                case Some(offset) => offset.inc[F]
                case None         => Offset.min.pure[F]
              }
              .map { offset => (partition, offset) }
        }
        .map { _.toMap }

      _ <- HeadCacheConsumption
        .apply(topic = topic, pointers = pointers, consumer = consumer, log = log)
        .foreach { records =>
          for {
            now <- Clock[F].realTime
            result <- records
              .values
              .parFoldMap1 {
                case (topicPartition, records) =>
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
                      val partition = topicPartition.partition
                      cachesMap
                        .get(partition)
                        .fold {
                          JournalError(s"invalid partition: $partition").raiseError[F, Sample]
                        } { cache =>
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
                        .foldLeft(none[Long]) {
                          case (timestamp, (_, records)) =>
                            records
                              .foldLeft(timestamp) {
                                case (timestamp, record) =>
                                  record
                                    .timestampAndType
                                    .fold {
                                      timestamp
                                    } { timestampAndType =>
                                      val timestamp1 = timestampAndType
                                        .timestamp
                                        .toEpochMilli
                                      timestamp.fold { timestamp1 } { _.min(timestamp1) }.some
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
          a <- caches.foldMapM { case (_, value) => value.meters }
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

        def get(id: String, partition: Partition, offset: Offset): F[PartitionCache.Result[F]] = {
          cachesMap
            .get(partition)
            .fold {
              JournalError(s"invalid partition: $partition").raiseError[F, PartitionCache.Result[F]]
            } { cache =>
              cache.get(id, offset)
            }
        }
      }
    }
  }

  /** Lightweight wrapper over [[KafkaConsumer]].
    *
    * Allows easier stubbing in unit tests and provides a little bit more
    * convenient [[TopicCache]]-specific API.
    */
  trait Consumer[F[_]] {

    /** Assigns specific topic partitions to a consumer.
     *
     * I.e. consumer groups will not be used.
     *
     * @see
     *   [[KafkaConsumer#assign]] for more details.
     */
    def assign(topic: Topic, partitions: Nes[Partition]): F[Unit]

    /** Moves fetching position to a different offset(s).
      *
      * The read will start from the new offsets the next time [[KafkaConsumer#poll]] is
      * called.
      *
      * @see
      *   [[KafkaConsumer#seek]] for more details.
      */
    def seek(topic: Topic, offsets: Nem[Partition, Offset]): F[Unit]

    /** Fetch data from the previously assigned partitions.
      *
      * @see
      *   [[KafkaConsumer#poll]] for more details.
      */
    def poll: F[ConsumerRecords[String, Unit]]

    /** Get the set of partitions for a given topic.
      *
      * @see
      *   [[KafkaConsumer#partitions]] for more details.
      */
    def partitions(topic: Topic): F[Set[Partition]]
  }

  object Consumer {

    /** Stub implemenation of [[Consumer]], which never returns any records. */
    def empty[F[_]: Applicative]: Consumer[F] = {
      class Empty
      new Empty with Consumer[F] {

        def assign(topic: Topic, partitions: Nes[Partition]): F[Unit] = ().pure[F]

        def seek(topic: Topic, offsets: Nem[Partition, Offset]): F[Unit] = ().pure[F]

        def poll: F[ConsumerRecords[Tag, Unit]] = ConsumerRecords.empty[String, Unit].pure[F]

        def partitions(topic: Topic): F[Set[Partition]] = Set.empty[Partition].pure[F]
      }
    }

    def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

    /** Wraps existing [[KafkaConsumer]] into [[Consumer]] API.
      *
      * @param consumer Previously created [[KafkaConsumer]].
      * @param pollTimeout The timeout to use for [[KafkaConsumer#poll]].
      */
    def apply[F[_]: Monad](
      consumer: KafkaConsumer[F, String, Unit],
      pollTimeout: FiniteDuration,
    ): Consumer[F] = {

      class Main
      new Main with Consumer[F] {

        def assign(topic: Topic, partitions: Nes[Partition]): F[Unit] = {
          val partitions1 = partitions.map { partition =>
            TopicPartition(topic = topic, partition)
          }
          consumer.assign(partitions1)
        }

        def seek(topic: Topic, offsets: Nem[Partition, Offset]): F[Unit] = {
          offsets.toNel.foldMapM {
            case (partition, offset) =>
              val topicPartition = TopicPartition(topic = topic, partition = partition)
              consumer.seek(topicPartition, offset)
          }
        }

        val poll: F[ConsumerRecords[Tag, Unit]] = consumer.poll(pollTimeout)

        def partitions(topic: Topic): F[Set[Partition]] = consumer.partitions(topic)
      }
    }

    /** Creates a new [[KafkaConsumer]] and wraps it into [[Consumer]] API.
      *
      * @param config
      *   Kafka configuration in form of [[ConsumerConfig]]. It is used to get
      *   Kafka address, mostly, and some important parameters will be ignored,
      *   as these need to be set to specific values for the cache to work. I.e.
      *   `autoOffsetReset`, `groupId` and `autoCommit` will not be used.
      * @param pollTimeout
      *   The timeout to use for [[KafkaConsumer#poll]].
      */
    def make[F[_]: Monad: KafkaConsumerOf: FromTry](
      config: ConsumerConfig,
      pollTimeout: FiniteDuration = 10.millis,
    ): Resource[F, Consumer[F]] = {
      val config1 = config.copy(autoOffsetReset = AutoOffsetReset.Earliest, groupId = None, autoCommit = false)
      for {
        consumer <- KafkaConsumerOf[F].apply[String, Unit](config1)
      } yield {
        Consumer[F](consumer, pollTimeout)
      }
    }

    private abstract sealed class WithLog

    implicit class ConsumerOps[F[_]](val self: Consumer[F]) extends AnyVal {

      /** Log debug messages on every call to the class methods.
        *
        * The messages will go to DEBUG level, so it is also necessary to enable
        * it in logger configuration.
        */
      def withLog(log: Log[F])(implicit F: Monad[F]): Consumer[F] = {
        new WithLog with Consumer[F] {

          def assign(topic: Topic, partitions: Nes[Partition]): F[Unit] = {
            for {
              _ <- log.debug(s"assign topic: $topic, partitions: $partitions")
              a <- self.assign(topic, partitions)
            } yield a
          }

          def seek(topic: Topic, offsets: Nem[Partition, Offset]): F[Unit] = {
            for {
              _ <- log.debug(s"seek topic: $topic, offsets: $offsets")
              a <- self.seek(topic, offsets)
            } yield a
          }

          def poll: F[ConsumerRecords[Tag, Unit]] = {
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

          def partitions(topic: Topic): F[Set[Partition]] = {
            for {
              a <- self.partitions(topic)
              _ <- log.debug(s"partitions topic: $topic, result: $a")
            } yield a
          }
        }
      }
    }
  }

  /** Cumulative average of some data stream.
    *
    * If one has to calcuate an average for a large list of numbers, one does
    * not have to keep all these numbers in a memory. It is enough to keep sum
    * of them and the count.
    *
    * @param sum
    *   Sum of all numbers seen.
    * @param count
    *   Number of all numbers seen.
    *
    * Example:
    * {{{
    * scala> import cats.syntax.all.*
    * scala> (1L to 100L).toList.map(Sample(_)).combineAll.avg
    * val res0: Option[Long] = Some(50)
    * }}}
    *
    * @see
    *   https://en.wikipedia.org/wiki/Moving_average#Cumulative_average
    */
  private final case class Sample(sum: Long, count: Int)

  private object Sample {

    /** Single number in a stream we are calculating average for */
    def apply(value: Long): Sample = Sample(sum = value, count = 1)

    /** Initial state of cumulative average, i.e. no numbers registered */
    val Empty: Sample = Sample(0L, 0)

    implicit val monoidSample: Monoid[Sample] = new Monoid[Sample] {

      def empty: Sample = Empty

      def combine(a: Sample, b: Sample): Sample = {
        Sample(sum = a.sum.combine(b.sum), count = a.count.combine(b.count))
      }
    }

    implicit class SampleOps(val self: Sample) extends AnyVal {

      /** Average of the all numbers seen, or `None` if no numbers were added.
        *
        * @return Average of all numbers seen, rounded down.
        */
      def avg: Option[Long] = {
        if (self.count > 0) (self.sum / self.count).some else none
      }
    }
  }

  private sealed abstract class WithMetrics

  private sealed abstract class WithLog

  implicit class TopicCacheOps[F[_]](val self: TopicCache[F]) extends AnyVal {

    /** Wrap instance in a class, which logs metrics to [[HeadCache.Metrics]] */
    def withMetrics(
      topic: Topic,
      metrics: HeadCache.Metrics[F],
    )(implicit F: MonadThrowable[F], measureDuration: MeasureDuration[F]): TopicCache[F] = {
      new WithMetrics with TopicCache[F] {

        def get(id: String, partition: Partition, offset: Offset): F[PartitionCache.Result[F]] = {
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

    /** Log debug messages on every call to a cache.
      *
      * The messages will go to DEBUG level, so it is also necessary to enable
      * it in logger configuration.
      */
    def withLog(log: Log[F])(implicit F: FlatMap[F], measureDuration: MeasureDuration[F]): TopicCache[F] = {
      new WithLog with TopicCache[F] {

        def get(id: String, partition: Partition, offset: Offset): F[PartitionCache.Result[F]] = {
          for {
            d <- MeasureDuration[F].start
            a <- self.get(id, partition, offset)
            d <- d
            _ <- log.debug(s"get in ${d.toMillis}ms, id: $id, offset: $partition:$offset, result: $a")
          } yield a
        }
      }
    }
  }

  private final implicit class SetOps[A](val self: Set[A]) extends AnyVal {

    /** Aggregate all values in a set to something else using [[Monoid]].
      *
      * In other words, provides `foldMapM` method to `Set`.
      *
      * The method is not provided directly by `cats-core`,
      * because it is unlawful.
      *
      * It is possible to achieve the same using `alleycats-core`
      * library like this, so the method might be removed in future:
      * {{{
      * scala> import cats.syntax.all.*
      * scala> import alleycats.std.all.*
      * scala> Set(1, 2, 3).foldMapM(_.some)
      * val res0: Option[Int] = Some(6)
      * }}}
      */
    def foldMapM[F[_]: Monad, B: Monoid](f: A => F[B]): F[B] = {
      self.foldLeft(Monoid[B].empty.pure[F]) {
        case (b0, a) =>
          for {
            b0 <- b0
            b1 <- f(a)
          } yield b0.combine(b1)
      }
    }

  }

  private final implicit class MapOps[K, V](val self: Map[K, V]) extends AnyVal {
    def foldMapM[F[_]: Monad, A: Monoid](f: (K, V) => F[A]): F[A] = {
      self.foldLeft(Monoid[A].empty.pure[F]) {
        case (a, (k, v)) =>
          a.flatMap { a => f(k, v).map { b => a.combine(b) } }
      }
    }
  }
}
