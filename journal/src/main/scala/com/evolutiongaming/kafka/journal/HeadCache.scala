package com.evolutiongaming.kafka.journal

import cats._
import cats.effect._
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal.cache.Cache
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, TopicPointers}
import com.evolutiongaming.kafka.journal.retry.Retry
import com.evolutiongaming.kafka.journal.util.EitherHelper._
import com.evolutiongaming.kafka.journal.util._
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords}
import com.evolutiongaming.skafka.{Offset, Partition, Topic, TopicPartition}
import com.evolutiongaming.concurrent.async

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

/**
  * TODO
  * 1. handle cancellation in case of timeouts and not leak memory
  * 2. Journal should close HeadCache
  * 3. Remove half of partition cache on cleanup
  * 4. Support configuration
  * 5. Add Metrics
  * 6. Clearly handle cases when topic is not yet created, but requests are coming
  * 7. Keep 1000 last seen entries, even if replicated.
  * 8. Fail headcache when background tasks failed
  */
trait HeadCache[F[_]] {
  import HeadCache._

  def apply(key: Key, partition: Partition, offset: Offset): F[Option[Result]]

  def close: F[Unit]
}


object HeadCache {

  def empty[F[_] : Applicative]: HeadCache[F] = new HeadCache[F] {

    def apply(key: Key, partition: Partition, offset: Offset) = Applicative[F].pure(None)

    def close = Applicative[F].unit
  }


  def of[F[_] : Concurrent : Eventual : Par : Timer : Log](
    consumer: F[Consumer[F]],
    config: Config = Config.Default): F[HeadCache[F]] = {

    for {
      cache <- Cache.of[F, Topic, TopicCache[F]]
      cache <- Ref.of(cache.pure[F])
    } yield {

      def topicCache(topic: Topic)(implicit log: Log[F]) = {
        val logTopic = log.prefixed(topic)
        for {
          _ <- logTopic.info("create")
          consumerWithLog = for {
            consumer <- consumer
          } yield {
            Consumer(consumer, logTopic)
          }
          topicCache <- TopicCache.of(
            topic = topic,
            config = config,
            consumer = consumerWithLog)(Concurrent[F], Eventual[F], Par[F], logTopic, TimerOf[F])
        } yield {
          topicCache
        }
      }

      new HeadCache[F] {

        def apply(key: Key, partition: Partition, offset: Offset) = {
          val topic = key.topic
          for {
            cache <- cache.get
            cache <- cache
            topicCache <- cache.getOrUpdate(topic)(topicCache(topic))
            result <- topicCache(id = key.id, partition = partition, offset = offset)
          } yield {
            result
          }
        }

        def close = {
          for {
            _ <- cache.get.flatten
            c <- cache.modify { c =>
              val cc = for {
                _ <- c
                c <- Concurrent[F].raiseError[Cache[F, Topic, TopicCache[F]]](ClosedException)
              } yield c
              (cc, c)
            }
            c <- c
            v <- c.values
            _ <- Par[F].foldMap(v.values) { v =>
              for {
                v <- v.get
                _ <- v.close
              } yield {}
            }
          } yield {}
        }
      }
    }
  }


  final case class Config(
    pollTimeout: FiniteDuration = 50.millis,
    cleanInterval: FiniteDuration = 3.seconds,
    maxSize: Int = 100000) {

    require(maxSize >= 1, s"maxSize($maxSize) >= 1")
  }

  object Config {
    val Default: Config = Config()
  }


  final case class Result(seqNr: Option[SeqNr], deleteTo: Option[SeqNr])

  object Result {
    val Empty: Result = Result(seqNr = None, deleteTo = None)
  }


  trait TopicCache[F[_]] {

    def apply(id: Id, partition: Partition, offset: Offset): F[Option[Result]]

    def close: F[Unit]
  }

  object TopicCache {

    type Listener[F[_]] = Map[Partition, PartitionEntry] => Option[F[Unit]]

    def of[F[_] : Concurrent : Eventual : Par : Log : Timer](
      topic: Topic,
      config: Config,
      consumer: F[Consumer[F]]): F[TopicCache[F]] = {

      for {
        pointers <- Eventual[F].pointers(topic)
        entries = for {
          (partition, offset) <- pointers.values
        } yield {
          val entry = PartitionEntry(partition = partition, offset = offset, entries = Map.empty, trimmed = None)
          (entry.partition, entry)
        }
        state <- SerialRef.of(State[F](entries, List.empty))
        consuming <- {

          def entriesOf(records: Map[TopicPartition, List[ConsumerRecord[Id, Bytes]]]) = {
            for {
              (partition, records) <- records
            } yield {
              val entries = for {
                (key, records) <- records.groupBy(_.key)
                id <- key
              } yield {

                case class OffsetAndHeader(offset: Offset, header: ActionHeader.AppendOrDelete)

                val offsetsAndHeaders = for {
                  record <- records
                  header <- record.toActionHeader
                  header <- PartialFunction.condOpt(header) {
                    case header: ActionHeader.Append => header
                    case header: ActionHeader.Delete => header
                  }
                } yield {
                  OffsetAndHeader(record.offset, header)
                }

                val offset = offsetsAndHeaders.foldLeft(Offset.Min) { _ max _.offset }

                val seqNr = offsetsAndHeaders.foldLeft(Option.empty[SeqNr]) { (b, a) =>
                  a.header match {
                    case a: ActionHeader.Append => Some(a.range.to max b)
                    case _: ActionHeader.Delete => b
                  }
                }

                val deleteTo = offsetsAndHeaders.foldLeft(Option.empty[SeqNr]) { (b, a) =>
                  a.header match {
                    case _: ActionHeader.Append => b
                    case a: ActionHeader.Delete => Some(a.to max b)
                  }
                }

                val entry = Entry(id = id.value, offset = offset, seqNr = seqNr, deleteTo = deleteTo)
                (entry.id, entry)
              }

              // TODO
              val offset = records.foldLeft(Offset.Min) { _ max _.offset }
              val partitionEntry = PartitionEntry(
                partition = partition.partition,
                offset = offset,
                entries = entries,
                trimmed = None /*TODO*/)
              (partitionEntry.partition, partitionEntry)
            }
          }

          Concurrent[F].start {

            val consuming = ConsumeTopic(
              topic = topic,
              from = pointers.values,
              pollTimeout = config.pollTimeout,
              consumer = consumer) { records =>
              
              val entries = entriesOf(records.values)
              state.update { state =>
                val combined = {

                  val combined = state.entries |+| entries

                  def sizeOf(map: Map[Partition, PartitionEntry]) = {
                    map.values.foldLeft(0) { _ + _.entries.size }
                  }

                  val maxSize = config.maxSize

                  if (sizeOf(combined) <= maxSize) {
                    combined
                  } else {
                    val partitions = combined.size
                    val maxSizePartition = maxSize / partitions max 1
                    for {
                      (partition, partitionEntry) <- combined
                    } yield {
                      val updated = {
                        if (partitionEntry.entries.size <= maxSizePartition) {
                          partitionEntry
                        } else {
                          // TODO
                          val offset = partitionEntry.entries.values.foldLeft(Offset.Min) { _ max _.offset }
                          // TODO remove half
                          partitionEntry.copy(entries = Map.empty, trimmed = Some(offset))
                        }
                      }
                      (partition, updated)
                    }
                  }
                }

                val zero = (List.empty[Listener[F]], List.empty[F[Unit]])
                val (listeners, completed) = state.listeners.foldLeft(zero) { case ((listeners, completed), listener) =>
                  listener(combined) match {
                    case None         => (listener :: listeners, completed)
                    case Some(result) => (listeners, result :: completed)
                  }
                }

                for {
                  _ <- Par[F].sequence(completed)
                } yield {
                  state.copy(entries = combined, listeners = listeners)
                }
              }
            }

            consuming.onError { case error =>
              Log[F].error(s"consuming failed with $error", error)
            }
          }
        }

        cleaning <- Concurrent[F].start {
          val cleaning = {
            for {
              _ <- TimerOf[F].sleep(config.cleanInterval)
              pointers <- Eventual[F].pointers(topic)
              before <- state.get
              _ <- state.update { _.removeUntil(pointers.values).pure[F] }
              after <- state.get
              removed = before.size - after.size
              _ <- if (removed > 0) Log[F].debug(s"remove $removed entries") else ().pure[F]
            } yield {}
          }
          cleaning.foreverM[Unit].onError { case error =>
            Log[F].error(s"cleaning failed with $error", error) // TODO fail head cache
          }
        }
      } yield {
        val cancel = for {
          _ <- Par[F].foldMap(List(consuming, cleaning)) { _.cancel }
        } yield {}
        apply(topic, cancel, state)
      }
    }

    def apply[F[_] : Concurrent : Eventual : Monad : Log](
      topic: Topic,
      cancel: F[Unit],
      stateRef: SerialRef[F, State[F]]): TopicCache[F] = {

      // TODO handle case with replicator being down

      new TopicCache[F] {

        def apply(id: Id, partition: Partition, offset: Offset) = {

          sealed trait Error

          object Error {
            case object Trimmed extends Error
            case object Invalid extends Error
            case object Behind extends Error
          }

          def entryOf(entries: Map[Partition, PartitionEntry]): Option[Option[Result]] = {
            val result = for {
              pe <- entries.get(partition) toRight Error.Invalid
              _ <- pe.offset >= offset trueOr Error.Behind
              r <- pe.entries.get(id).fold {
                // TODO Test this
                // TODO
                //                  val replicatedTo: Offset = ???
                //
                //                  if (offset <= replicatedTo) {
                //                    Result(None, None).asRight
                //                  } else if (partitionEntry.trimmed.) {
                for {
                  _ <- pe.trimmed.isEmpty trueOr Error.Trimmed
                } yield Result.Empty
              } { e =>
                Result(seqNr = e.seqNr, deleteTo = e.deleteTo).asRight
              }
            } yield r


            result match {
              case Right(result)       => result.some.some
              case Left(Error.Behind)  => none
              case Left(Error.Trimmed) => none.some
              case Left(Error.Invalid) => none
            }
          }

          for {
            state <- stateRef.get
            result <- entryOf(state.entries).fold {
              for {
                result <- stateRef.modify { state =>
                  entryOf(state.entries).fold {
                    for {
                      deferred <- Deferred[F, Option[Result]]
                      listener = (entries: Map[Partition, PartitionEntry]) => {
                        for {
                          r <- entryOf(entries)
                        } yield for {
                          _ <- deferred.complete(r)
                          _ <- Log[F].debug(s"remove listener, id: $id, offset: $partition:$offset")
                        } yield {}
                      }
                      _ <- Log[F].debug(s"add listener, id: $id, offset: $partition:$offset")
                    } yield {
                      val stateNew = state.copy(listeners = listener :: state.listeners)
                      (stateNew, deferred.get)
                    }
                  } { entry =>
                    (state, entry.pure[F]).pure[F]
                  }
                }
                result <- result
              } yield result
            } {
              _.pure[F]
            }
          } yield result
        }

        def close = {
          for {
            _ <- Log[F].debug("close")
            _ <- cancel // TODO should be idempotent
          } yield {}
        }
      }
    }


    // TODO both seqNr & deleteTo cannot be None
    final case class Entry(id: Id, offset: Offset, seqNr: Option[SeqNr], deleteTo: Option[SeqNr])

    object Entry {

      implicit val SemigroupImpl: Semigroup[Entry] = new Semigroup[Entry] {

        def combine(x: Entry, y: Entry) = {
          val seqNr = x.seqNr max y.seqNr
          val deleteTo = x.deleteTo max y.deleteTo
          val offset = x.offset max y.offset
          x.copy(seqNr = seqNr, deleteTo = deleteTo, offset = offset)
        }
      }
    }


    final case class PartitionEntry(
      partition: Partition,
      offset: Offset,
      entries: Map[Id, Entry],
      trimmed: Option[Offset] /*TODO remove this field*/)

    object PartitionEntry {

      implicit val SemigroupImpl: Semigroup[PartitionEntry] = new Semigroup[PartitionEntry] {

        def combine(x: PartitionEntry, y: PartitionEntry) = {
          val entries = x.entries |+| y.entries
          val offset = x.offset max y.offset
          x.copy(entries = entries, offset = offset)
        }
      }
    }

    final case class State[F[_]](
      entries: Map[Partition, PartitionEntry],
      listeners: List[Listener[F]]) {

      def size: Long = entries.values.foldLeft(0l) { _ + _.entries.size }

      def removeUntil(pointers: Map[Partition, Offset]): State[F] = {
        val updated = for {
          (partition, offset) <- pointers
          partitionEntry <- entries.get(partition)
        } yield {
          val entries = for {
            (id, entry) <- partitionEntry.entries
            if entry.offset > offset
          } yield {
            (id, entry)
          }
          val trimmed = partitionEntry.trimmed.filter(_ > offset)
          val updated = partitionEntry.copy(entries = entries, trimmed = trimmed)
          (partition, updated)
        }

        copy(entries = entries ++ updated)
      }
    }
  }


  trait Consumer[F[_]] {

    def assign(topic: Topic, partitions: Nel[Partition]): F[Unit]

    def seek(topic: Topic, offsets: Map[Partition, Offset]): F[Unit]

    def poll(timeout: FiniteDuration): F[ConsumerRecords[String, Bytes]]

    def partitions(topic: Topic): F[List[Partition]]

    def close: F[Unit]
  }

  object Consumer {

    def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

    def apply[F[_] : Applicative](consumer: KafkaConsumer[F])(implicit monoid: Monoid[F[Unit]]): Consumer[F] = {
      new Consumer[F] {

        def assign(topic: Topic, partitions: Nel[Partition]) = {
          val topicPartitions = for {
            partition <- partitions
          } yield {
            TopicPartition(topic = topic, partition)
          }
          consumer.assign(topicPartitions)
        }

        def seek(topic: Topic, offsets: Map[Partition, Offset]) = {
          offsets.foldMap { case (partition, offset) =>
            val topicPartition = TopicPartition(topic = topic, partition = partition)
            consumer.seek(topicPartition, offset)
          }
        }

        def poll(timeout: FiniteDuration) = consumer.poll(timeout)

        def partitions(topic: Topic) = consumer.partitions(topic)

        def close = consumer.close
      }
    }

    def apply[F[_] : Monad](consumer: Consumer[F], log: Log[F]): Consumer[F] = {

      new Consumer[F] {

        def assign(topic: Topic, partitions: Nel[Partition]) = {
          for {
            _ <- log.debug(s"assign topic: $topic, partitions: $partitions")
            r <- consumer.assign(topic, partitions)
          } yield r
        }

        def seek(topic: Topic, offsets: Map[Partition, Offset]) = {
          for {
            _ <- log.debug(s"seek topic: $topic, offsets: $offsets")
            r <- consumer.seek(topic, offsets)
          } yield r
        }

        def poll(timeout: FiniteDuration) = {
          for {
            r <- consumer.poll(timeout)
            _ <- {
              if (r.values.isEmpty) ().pure[F]
              else log.debug {
                val size = r.values.values.foldLeft(0l) { _ + _.size }
                s"poll timeout: $timeout, result: $size"
              }
            }
          } yield r
        }

        def partitions(topic: Topic) = {
          for {
            r <- consumer.partitions(topic)
            _ <- log.debug(s"partitions topic: $topic, result: $r")
          } yield {
            r
          }
        }

        def close = {
          for {
            _ <- log.debug("close")
            r <- consumer.close
          } yield r
        }
      }
    }


    def resource[F[_] : Functor](consumer: F[Consumer[F]]): Resource[F, Consumer[F]] = {
      Resource.make(consumer)(_.close)
    }
  }


  trait Eventual[F[_]] {
    def pointers(topic: Topic): F[TopicPointers]
  }

  object Eventual {

    def apply[F[_]](implicit F: Eventual[F]): Eventual[F] = F

    def apply[F[_]: FromFuture](eventualJournal: EventualJournal[async.Async]): Eventual[F] = {
      new HeadCache.Eventual[F] {
        def pointers(topic: Topic) = {
          FromFuture[F].apply {
            eventualJournal.pointers(topic).future
          }
        }
      }
    }

    def empty[F[_] : Applicative]: Eventual[F] = const(Applicative[F].pure(TopicPointers.Empty))

    def const[F[_] : Applicative](value: F[TopicPointers]): Eventual[F] = new Eventual[F] {
      def pointers(topic: Topic) = value
    }
  }


  object ConsumeTopic {

    def apply[F[_] : Sync : Timer : Log](
      topic: Topic,
      from: Map[Partition, Offset],
      pollTimeout: FiniteDuration,
      consumer: F[Consumer[F]])(
      onRecords: ConsumerRecords[String, Bytes] => F[Unit]): F[Unit] = {

      def poll(implicit consumer: Consumer[F]): F[Unit] = {
        for {
          records <- consumer.poll(pollTimeout)
          _       <- {
            if (records.values.isEmpty) {
              ().pure[F]
            } else {
              onRecords(records)
            }
          }
        } yield {}
      }

      def partitionsOf(implicit consumer: Consumer[F]): F[Nel[Partition]] = {

        val onError = (error: Throwable, details: Retry.Details) => {
          import Retry.Decision

          def prefix = s"consumer.partitions($topic) failed"

          details.decision match {
            case Decision.Retry(delay) =>
              Log[F].error(s"$prefix, retrying in $delay, error: $error")

            case Decision.GiveUp =>
              val retries = details.retries
              Log[F].error(s"$prefix, retried $retries times, error: $error", error)
          }
        }

        val partitions = for {
          partitions <- consumer.partitions(topic)
          partitions <- Nel.opt(partitions) match {
            case Some(a) => a.pure[F]
            case None    => NoPartitionsException.raiseError[F, Nel[Partition]]
          }
        } yield partitions

        implicit val clock = TimerOf[F].clock

        for {
          rng        <- Rng.fromClock[F]
          strategy    = {
            val strategy = Retry.Strategy.fullJitter(3.millis, rng)
            Retry.Strategy.cap(300.millis, strategy)
          }
          partitions <- Retry(strategy, onError)(partitions)
        } yield {
          partitions
        }
      }

      Consumer.resource(consumer).use { implicit consumer =>
        for {
          partitions <- partitionsOf
          _          <- consumer.assign(topic, partitions)
          offsets     = for {
            partition <- partitions
          } yield {
            val offset = from.get(partition).fold(Offset.Min)(_ + 1l)
            (partition, offset)
          }
          _          <- consumer.seek(topic, offsets.toMap)
          _          <- poll.foreverM[Unit]
        } yield {}
      }
    }
  }


  case object NoPartitionsException extends RuntimeException("No partitions") with NoStackTrace

  case object ClosedException extends RuntimeException("HeadCache is closed") with NoStackTrace
}