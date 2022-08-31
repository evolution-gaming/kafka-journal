package com.evolutiongaming.kafka.journal

import cats._
import cats.data.{OptionT, NonEmptyList => Nel, NonEmptyMap => Nem, NonEmptySet => Nes}
import cats.effect._
import cats.effect.implicits._
import cats.syntax.all._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.catshelper._
import com.evolutiongaming.kafka.journal.conversions.ConsRecordToActionHeader
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, TopicPointers}
import com.evolutiongaming.kafka.journal.util.TemporalHelper._
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.Retry.implicits._
import com.evolutiongaming.retry.{Sleep, Strategy}
import com.evolutiongaming.scache.{Cache, Releasable}
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, ConsumerRecord, ConsumerRecords}
import com.evolutiongaming.skafka.{Offset, Partition, Topic, TopicPartition}
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics._

import java.time.Instant
import scala.concurrent.duration._

/**
 * TODO headcache:
 * 1. handle cancellation in case of timeouts and not leak memory
 * 2. Remove half of partition cache on cleanup
 * 3. Clearly handle cases when topic is not yet created, but requests are coming
 * 4. Keep 1000 last seen entries, even if replicated.
 * 5. Fail headcache when background tasks failed
 */
trait HeadCache[F[_]] {

  def get(key: Key, partition: Partition, offset: Offset): F[Either[HeadCacheError, HeadInfo]]
}


object HeadCache {

  def empty[F[_]: Applicative]: HeadCache[F] = const(HeadCacheError.invalid.asLeft[HeadInfo].pure[F])


  def const[F[_]](value: F[Either[HeadCacheError, HeadInfo]]): HeadCache[F] = {
    (_: Key, _: Partition, _: Offset) => value
  }


  def of[F[_]: Temporal: Runtime: LogOf: KafkaConsumerOf: MeasureDuration: FromTry: FromJsResult: JsonCodec.Decode](
    consumerConfig: ConsumerConfig,
    eventualJournal: EventualJournal[F],
    metrics: Option[HeadCacheMetrics[F]]
  ): Resource[F, HeadCache[F]] = {

    val eventual = Eventual(eventualJournal)
    val consumer = Consumer.of[F](consumerConfig)
    for {
      log       <- LogOf[F].apply(HeadCache.getClass).toResource
      headCache <- HeadCache.of(eventual, log, consumer, metrics)
    } yield {
      metrics.fold { headCache } { metrics => headCache.withMetrics(metrics.headCache) }
    }
  }


  def of[F[_]: Temporal: Runtime: FromJsResult: MeasureDuration: JsonCodec.Decode](
    eventual: Eventual[F],
    log: Log[F],
    consumer: Resource[F, Consumer[F]],
    metrics: Option[HeadCacheMetrics[F]],
    config: HeadCacheConfig = HeadCacheConfig.default
  ): Resource[F, HeadCache[F]] = {

    implicit val consRecordToActionHeader = ConsRecordToActionHeader[F]
    implicit val consRecordToKafkaRecord = HeadCache.ConsRecordToKafkaRecord[F]

    def headCache(cache: Cache[F, Topic, TopicCache[F]]) = {

      def topicCache(topic: Topic) = {
        val logTopic = log.prefixed(topic)
        val topicCache = for {
          _          <- logTopic.info("create").toResource
          consumer1   = consumer.map(Consumer(_, logTopic))
          topicCache <- TopicCache.of(
            topic = topic,
            config = config,
            eventual = eventual,
            consumer = consumer1,
            metrics = metrics.fold(Metrics.empty[F])(_.headCache),
            log = logTopic)
        } yield topicCache

        Releasable.of(topicCache)
      }

      new HeadCache[F] {

        def get(key: Key, partition: Partition, offset: Offset) = {
          val topic = key.topic
          for {
            topicCache <- cache.getOrUpdateReleasable(topic)(topicCache(topic))
            result     <- topicCache.get(id = key.id, partition = partition, offset = offset)
          } yield result
        }
      }
    }

    val result = for {
      cache <- Cache.loading[F, Topic, TopicCache[F]]
      cache <- metrics.fold(cache.pure[Resource[F, *]]) { metrics => cache.withMetrics(metrics.cache) }
    } yield {
      headCache(cache)
    }

    result.withFence
  }


  trait TopicCache[F[_]] {

    def get(id: String, partition: Partition, offset: Offset): F[Either[HeadCacheError, HeadInfo]]
  }

  object TopicCache {

    def of[F[_]: Temporal](
      topic: Topic,
      config: HeadCacheConfig,
      eventual: Eventual[F],
      consumer: Resource[F, Consumer[F]],
      metrics: Metrics[F],
      log: Log[F])(implicit
      consRecordToKafkaRecord: ConsRecordToKafkaRecord[F]
    ): Resource[F, TopicCache[F]] = {

      def consume(stateRef: Ref[F, State[F]], pointers: F[Map[Partition, Offset]]) = {

        val stream = HeadCacheConsumption(
          topic = topic,
          pointers = pointers,
          consumer = consumer,
          log = log)

        stream
          .foreach { records0 =>
            val records = records0.toMap

            def measureRound(state: State[F], now: Instant) = {
              val latency = records.values.foldLeft(0.millis) { case (latency, records) =>
                latency max (now diff records.head.timestamp)
              }
              metrics.round(
                topic = topic,
                entries = state.size,
                listeners = state.listeners.size,
                deliveryLatency = latency)
            }

            def combineAndRun(state: State[F], entries: Map[Partition, PartitionEntry]) = {
              val combined = combineAndTrim(state.entries, entries, config.maxSize)
              val (listeners, completed) = runListeners(state.listeners, combined)
              val state1  = state.copy(entries = combined, listeners = listeners)
              (state1, (state1, completed))
            }

            for {
              now     <- Clock[F].instant
              entries  = partitionEntries(records)
              ab      <- stateRef.modify { state => combineAndRun(state, entries) }
              (state, completed) = ab
              _       <- completed.parFold
              _       <- measureRound(state, now)
            } yield {}
          }
          .onError { case error => log.error(s"consuming failed with $error", error)} /*TODO headcache: fail head cache*/
      }

      def cleaning(stateRef: Ref[F, State[F]]) = {

        val cleaning = for {
          _        <- Sleep[F].sleep(config.cleanInterval)
          pointers <- eventual.pointers(topic)
          before   <- stateRef.get
          _        <- stateRef.update { _.removeUntil(pointers.values) }
          after    <- stateRef.get
          removed   = before.size - after.size
          _        <- if (removed > 0) log.debug(s"remove $removed entries") else ().pure[F]
        } yield {}

        for {
          random   <- Random.State.fromClock[F]()
          strategy  = Strategy
            .exponential(10.millis)
            .cap(3.seconds)
            .jitter(random)
          _        <- cleaning
            .retry(strategy)
            .handleErrorWith { e => log.error(s"cleaning failed with $e", e) }
            .foreverM[Unit]
        } yield {}
      }

      def state(pointers: TopicPointers) = {
        val entries = for {
          (partition, offset) <- pointers.values
        } yield {
          val partitionEntry = PartitionEntry(offset = offset, entries = Map.empty, trimmed = None)
          (partition, partitionEntry)
        }
        State(entries, Set.empty[Listener[F]])
      }

      def stateRef(state: State[F]) = {
        Resource.make {
          Ref[F].of(state)
        } { stateRef =>

          def removeListeners(state: State[F]) = {
            val state1 = state.copy(listeners = Set.empty[Listener[F]])
            (state1, state.listeners)
          }

          for {
            listeners <- stateRef.modify { state => removeListeners(state) }
            _         <- listeners.toList.parFoldMap { _.release }
          } yield {}
        }
      }

      for {
        pointers <- eventual.pointers(topic).toResource
        stateRef <- stateRef(state(pointers))
        pointers  = stateRef.get.map(_.pointers)
        _        <- consume(stateRef, pointers).background
        _        <- cleaning(stateRef).background
      } yield {
        apply(topic, stateRef, metrics, log, config.timeout)
      }
    }

    def apply[F[_]: Temporal](
      topic: Topic,
      stateRef: Ref[F, State[F]],
      metrics: Metrics[F],
      log: Log[F],
      timeout: FiniteDuration
    ): TopicCache[F] = {

      // TODO headcache: handle case with replicator being down

      new TopicCache[F] {

        def get(id: String, partition: Partition, offset: Offset) = {

          sealed abstract class Error

          object Error {
            case object Trimmed extends Error
            case object Invalid extends Error
            case object Behind extends Error
          }

          def headInfoOf(entries: Map[Partition, PartitionEntry]): Option[Option[HeadInfo]] = {
            val result = for {
              partitionEntry <- entries.get(partition) toRight Error.Invalid
              _              <- partitionEntry.offset >= offset trueOr Error.Behind
              result         <- partitionEntry.entries.get(id).fold {
                // TODO headcache: Test this
                //                  val replicatedTo: Offset =
                //
                //                  if (offset <= replicatedTo) {
                //                    Result(None, None).asRight
                //                  } else if (partitionEntry.trimmed.) {
                for {
                  _ <- partitionEntry.trimmed.isEmpty trueOr Error.Trimmed
                } yield {
                  HeadInfo.empty.some
                }
              } { entry =>
                entry.info.some.asRight
              }
            } yield result

            result match {
              case Right(result)       => result.some
              case Left(Error.Behind)  => none
              case Left(Error.Trimmed) => none.some
              case Left(Error.Invalid) => none
            }
          }

          def addListener(state: State[F]) = {

            def listenerOf(deferred: Deferred[F, F[Option[HeadInfo]]]) = new Listener[F] {

              def apply(entries: Map[Partition, PartitionEntry]) = {
                for {
                  r <- headInfoOf(entries)
                } yield for {
                  _ <- deferred.complete(r.pure[F])
                  _ <- log.debug(s"remove listener, id: $id, offset: $partition:$offset")
                } yield {}
              }

              def release = {
                deferred.complete(HeadCacheReleasedError.raiseError[F, Option[HeadInfo]]).void
              }
            }

            for {
              deferred <- Deferred[F, F[Option[HeadInfo]]]
              listener  = listenerOf(deferred)
            } yield {
              val stateNew = state.copy(listeners = state.listeners + listener)

              val result = deferred
                .get
                .flatten
                .onCancel { stateRef.update { state => state.copy(listeners = state.listeners - listener ) }  }
              
              (stateNew, result)
            }
          }

          def logListener(state: State[F]) =
            log.debug(s"add listener, id: $id, offset: $partition:$offset") *>
              metrics.listeners(topic, state.listeners.size)

          def result: F[Option[HeadInfo]] =
            stateRef.access.flatMap {
              case (state, set) =>
                headInfoOf(state.entries).fold {
                  for {
                    next         <- addListener(state)
                    (state, res) = next
                    isSet        <- set(state)
                    res          <- if (isSet) logListener(state) *> res else result
                  } yield res
                }(_.pure[F])
            }

          result
            .race(Sleep[F].sleep(timeout))
            .map {
              case Left(Some(a)) => a.asRight[HeadCacheError]
              case Left(None)    => HeadCacheError.invalid.asLeft[HeadInfo]
              case Right(_)      => HeadCacheError.timeout(timeout).asLeft[HeadInfo]
            }
        }
      }
    }


    private def combineAndTrim(
      x: Map[Partition, PartitionEntry],
      y: Map[Partition, PartitionEntry],
      maxSize: Int
    ): Map[Partition, PartitionEntry] = {

      val combined = x combine y
      for {
        (partition, partitionEntry) <- combined
      } yield {
        val updated = {
          if (partitionEntry.entries.size <= maxSize) {
            partitionEntry
          } else {
            val offset = partitionEntry.entries.values.foldLeft(Offset.min) { _ max _.offset }
            // TODO headcache: remove half
            partitionEntry.copy(entries = Map.empty, trimmed = offset.some)
          }
        }
        (partition, updated)
      }
    }


    private def partitionEntries(
      records: Map[Partition, Nel[KafkaRecord]]
    ): Map[Partition, PartitionEntry] = {

      for {
        (partition, records) <- records
      } yield {
        val entries = for {
          (id, records)  <- records.toList.groupBy { _.id }
          (info, offset)  = records.foldLeft((HeadInfo.empty, Offset.min)) { case ((info, offset), record) =>
            val info1 = info(record.header, record.offset)
            val offset1 = record.header match {
              case _: ActionHeader.AppendOrDelete => record.offset max offset
              case _: ActionHeader.Mark           => offset
            }
            (info1, offset1)
          }
          entry          <- info match {
            case HeadInfo.Empty          => none[Entry]
            case info: HeadInfo.NonEmpty => Entry(id = id, offset = offset, info).some
          }
        } yield {
          (entry.id, entry)
        }

        val offset = records.foldLeft(Offset.min) { _ max _.offset }
        val partitionEntry = PartitionEntry(
          offset = offset,
          entries = entries,
          trimmed = None /*TODO headcache*/)
        (partition, partitionEntry)
      }
    }


    private def runListeners[F[_]](
      listeners: Set[Listener[F]],
      entries: Map[Partition, PartitionEntry]
    ): (Set[Listener[F]], List[F[Unit]]) = {

      val zero = (Set.empty[Listener[F]], List.empty[F[Unit]])
      listeners.foldLeft(zero) { case ((listeners, results), listener) =>
        listener(entries) match {
          case None         => (listeners + listener, results)
          case Some(result) => (listeners, result :: results)
        }
      }
    }


    final case class Entry(id: String, offset: Offset, info: HeadInfo.NonEmpty)

    object Entry {

      implicit val semigroupEntry: Semigroup[Entry] = {
        (x: Entry, y: Entry) => {
          val offset = x.offset max y.offset
          val info = x.info combine y.info
          x.copy(info = info, offset = offset)
        }
      }
    }


    final case class PartitionEntry(
      offset: Offset,
      entries: Map[String, Entry],
      trimmed: Option[Offset] /*TODO headcache: remove this field*/)

    object PartitionEntry {

      implicit val semigroupPartitionEntry: Semigroup[PartitionEntry] = {
        (x: PartitionEntry, y: PartitionEntry) => {
          val entries = x.entries combine y.entries
          val offset = x.offset max y.offset
          x.copy(entries = entries, offset = offset)
        }
      }
    }


    final case class State[F[_]](entries: Map[Partition, PartitionEntry], listeners: Set[Listener[F]])

    object State {

      implicit class StateOps[F[_]](val self: State[F]) extends AnyVal {

        def size: Long = self.entries.values.foldLeft(0L) { _ + _.entries.size }

        def removeUntil(pointers: Map[Partition, Offset]): State[F] = {
          val entries = for {
            (partition, offset) <- pointers
            partitionEntry      <- self.entries.get(partition)
          } yield {
            val entries = partitionEntry.entries.filter { case (_, entry) => entry.offset > offset }
            val trimmed = partitionEntry.trimmed.filter { _ > offset }
            val updated = partitionEntry.copy(entries = entries, trimmed = trimmed)
            (partition, updated)
          }

          self.copy(entries = self.entries ++ entries)
        }

        def pointers: Map[Partition, Offset] = {
          for {
            (partition, entry) <- self.entries
          } yield {
            (partition, entry.offset)
          }
        }
      }
    }


    trait Listener[F[_]] {

      def apply(entries: Map[Partition, PartitionEntry]): Option[F[Unit]]

      def release: F[Unit]
    }
  }


  trait Consumer[F[_]] {

    def assign(topic: Topic, partitions: Nes[Partition]): F[Unit]

    def seek(topic: Topic, offsets: Nem[Partition, Offset]): F[Unit]

    def poll: F[ConsumerRecords[String, Unit]]

    def partitions(topic: Topic): F[Set[Partition]]
  }

  object Consumer {

    def empty[F[_]: Applicative]: Consumer[F] = new Consumer[F] {

      def assign(topic: Topic, partitions: Nes[Partition]) = ().pure[F]

      def seek(topic: Topic, offsets: Nem[Partition, Offset]) = ().pure[F]

      def poll = ConsumerRecords.empty[String, Unit].pure[F]

      def partitions(topic: Topic) = Set.empty[Partition].pure[F]
    }


    def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

    def apply[F[_]: Monad](
      consumer: KafkaConsumer[F, String, Unit],
      pollTimeout: FiniteDuration
    ): Consumer[F] = {

      new Consumer[F] {

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

    def apply[F[_]: Monad](consumer: Consumer[F], log: Log[F]): Consumer[F] = {

      new Consumer[F] {

        def assign(topic: Topic, partitions: Nes[Partition]) = {
          for {
            _ <- log.debug(s"assign topic: $topic, partitions: $partitions")
            r <- consumer.assign(topic, partitions)
          } yield r
        }

        def seek(topic: Topic, offsets: Nem[Partition, Offset]) = {
          for {
            _ <- log.debug(s"seek topic: $topic, offsets: $offsets")
            r <- consumer.seek(topic, offsets)
          } yield r
        }

        def poll = {
          for {
            r <- consumer.poll
            _ <- {
              if (r.values.isEmpty) ().pure[F]
              else log.debug {
                val size = r.values.values.foldLeft(0L) { _ + _.size }
                s"poll result: $size"
              }
            }
          } yield r
        }

        def partitions(topic: Topic) = {
          for {
            r <- consumer.partitions(topic)
            _ <- log.debug(s"partitions topic: $topic, result: $r")
          } yield r
        }
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
        HeadCache.Consumer[F](consumer, pollTimeout)
      }
    }
  }


  trait Eventual[F[_]] {

    def pointers(topic: Topic): F[TopicPointers]
  }

  object Eventual {

    def apply[F[_]](implicit F: Eventual[F]): Eventual[F] = F

    def apply[F[_]](eventualJournal: EventualJournal[F]): Eventual[F] = new HeadCache.Eventual[F] {
      def pointers(topic: Topic) = eventualJournal.pointers(topic)
    }

    def empty[F[_]: Applicative]: Eventual[F] = const(TopicPointers.empty.pure[F])

    def const[F[_]](value: F[TopicPointers]): Eventual[F] = new Eventual[F] {
      def pointers(topic: Topic) = value
    }
  }


  implicit class HeadCacheOps[F[_]](val self: HeadCache[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): HeadCache[G] = new HeadCache[G] {

      def get(key: Key, partition: Partition, offset: Offset) = {
        f(self.get(key, partition, offset))
      }
    }


    def withMetrics(
      metrics: Metrics[F])(implicit
      F: MonadThrowable[F],
      measureDuration: MeasureDuration[F]
    ): HeadCache[F] = new HeadCache[F] {

      def get(key: Key, partition: Partition, offset: Offset) = {

        def result(result: Either[Throwable, Either[HeadCacheError, HeadInfo]]) = {
          result match {
            case Right(Right(HeadInfo.Empty))           => Metrics.Result.Replicated
            case Right(Right(_: HeadInfo.NonEmpty))     => Metrics.Result.NotReplicated
            case Right(Left(HeadCacheError.Invalid))    => Metrics.Result.Invalid
            case Right(Left(_: HeadCacheError.Timeout)) => Metrics.Result.Timeout
            case Left(_)                                => Metrics.Result.Failure
          }
        }

        for {
          d <- MeasureDuration[F].start
          r <- self.get(key, partition, offset).attempt
          d <- d
          a = result(r)
          _ <- metrics.get(key.topic, d, a)
          r <- r.liftTo[F]
        } yield r
      }
    }


    def withLog(log: Log[F])(implicit F: FlatMap[F], measureDuration: MeasureDuration[F]): HeadCache[F] = new HeadCache[F] {

      def get(key: Key, partition: Partition, offset: Offset) = {
        for {
          d <- MeasureDuration[F].start
          r <- self.get(key, partition, offset)
          d <- d
          _ <- log.debug(s"$key get in ${ d.toMillis }ms, offset: $partition:$offset, result: $r")
        } yield r
      }
    }
  }


  implicit class HeadCacheResourceOps[F[_]](val self: Resource[F, HeadCache[F]]) extends AnyVal {

    def withFence(implicit F: Concurrent[F]): Resource[F, HeadCache[F]] = HeadCacheFenced.of(self)
  }


  trait Metrics[F[_]] {

    def get(topic: Topic, latency: FiniteDuration, result: Metrics.Result): F[Unit]

    def listeners(topic: Topic, size: Int): F[Unit]

    def round(topic: Topic, entries: Long, listeners: Int, deliveryLatency: FiniteDuration): F[Unit]
  }

  object Metrics {

    def empty[F[_]: Applicative]: Metrics[F] = const(().pure[F])


    def const[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def get(topic: Topic, latency: FiniteDuration, result: Metrics.Result) = unit

      def listeners(topic: Topic, size: Int) = unit

      def round(topic: Topic, entries: Long, listeners: Int, deliveryLatency: FiniteDuration) = unit
    }


    type Prefix = String

    object Prefix {
      val default: Prefix = "headcache"
    }


    def of[F[_]: Monad](
      registry: CollectorRegistry[F],
      prefix: Prefix = Prefix.default
    ): Resource[F, Metrics[F]] = {

      val quantiles = Quantiles(
        Quantile(0.9, 0.05),
        Quantile(0.99, 0.005))

      val getLatencySummary = registry.summary(
        name = s"${ prefix }_get_latency",
        help = "HeadCache get latency in seconds",
        quantiles = quantiles,
        labels = LabelNames("topic", "result"))

      val getResultCounter = registry.counter(
        name = s"${ prefix }_get_results",
        help = "HeadCache `get` call result: replicated, not_replicated, invalid or failure",
        labels = LabelNames("topic", "result"))

      val entriesGauge = registry.gauge(
        name = s"${ prefix }_entries",
        help = "HeadCache entries",
        labels = LabelNames("topic"))

      val listenersGauge = registry.gauge(
        name = s"${ prefix }_listeners",
        help = "HeadCache listeners",
        labels = LabelNames("topic"))

      val deliveryLatencySummary = registry.summary(
        name = s"${ prefix }_delivery_latency",
        help = "HeadCache kafka delivery latency in seconds",
        quantiles = quantiles,
        labels = LabelNames("topic"))

      for {
        getLatencySummary <- getLatencySummary
        getResultCounter <- getResultCounter
        entriesGauge <- entriesGauge
        listenersGauge <- listenersGauge
        deliveryLatencySummary <- deliveryLatencySummary
      } yield {

        new Metrics[F] {

          def get(topic: Topic, latency: FiniteDuration, result: Metrics.Result) = {

            val name = result match {
              case Result.Replicated    => "replicated"
              case Result.NotReplicated => "not_replicated"
              case Result.Invalid       => "invalid"
              case Result.Timeout       => "timeout"
              case Result.Failure       => "failure"
            }

            for {
              _ <- getLatencySummary.labels(topic, name).observe(latency.toNanos.nanosToSeconds)
              _ <- getResultCounter.labels(topic, name).inc()
            } yield {}
          }

          def listeners(topic: Topic, size: Int) = {
            listenersGauge.labels(topic).set(size.toDouble)
          }

          def round(topic: Topic, entries: Long, listeners: Int, deliveryLatency: FiniteDuration) = {
            for {
              _ <- entriesGauge.labels(topic).set(entries.toDouble)
              _ <- listenersGauge.labels(topic).set(listeners.toDouble)
              _ <- deliveryLatencySummary.labels(topic).observe(deliveryLatency.toNanos.nanosToSeconds)
            } yield {}
          }
        }
      }
    }


    sealed abstract class Result extends Product

    object Result {
      case object Replicated extends Result
      case object NotReplicated extends Result
      case object Invalid extends Result
      case object Timeout extends Result
      case object Failure extends Result
    }
  }


  final case class KafkaRecord(
    id: String,
    timestamp: Instant,
    offset: Offset,
    header: ActionHeader)


  trait ConsRecordToKafkaRecord[F[_]] {

    def apply(record: ConsumerRecord[String, Unit]): OptionT[F, KafkaRecord]
  }

  object ConsRecordToKafkaRecord {

    implicit def apply[F[_]: Monad](implicit
      consRecordToActionHeader: ConsRecordToActionHeader[F]
    ): ConsRecordToKafkaRecord[F] = {
      record: ConsumerRecord[String, Unit] => {

        for {
          key              <- record.key.toOptionT[F]
          timestampAndType <- record.timestampAndType.toOptionT[F]
          header           <- consRecordToActionHeader(record)
        } yield {
          KafkaRecord(
            key.value,
            timestampAndType.timestamp,
            record.offset,
            header)
        }
      }
    }
  }
}