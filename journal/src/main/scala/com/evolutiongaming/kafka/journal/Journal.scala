package com.evolutiongaming.kafka.journal

import cats._
import cats.arrow.FunctionK
import cats.effect._
import cats.implicits._
import com.evolutiongaming.kafka.journal.ClockHelper._
import com.evolutiongaming.kafka.journal.EventsSerializer._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.stream.FoldWhile.FoldWhileOps
import com.evolutiongaming.kafka.journal.stream.Stream
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerRecords}
import com.evolutiongaming.skafka.producer.{Acks, ProducerConfig, ProducerRecord}
import com.evolutiongaming.skafka.{Bytes => _, _}
import play.api.libs.json.JsValue

import scala.concurrent.duration._

trait Journal[F[_]] {

  def append(
    key: Key,
    events: Nel[Event],
    metadata: Option[JsValue] = None,
    headers: Headers = Headers.Empty
  ): F[PartitionOffset]

  def read(key: Key, from: SeqNr = SeqNr.Min): Stream[F, EventRecord]

  // TODO return Pointer and test it
  def pointer(key: Key): F[Option[SeqNr]]

  // TODO return Pointer and test it
  def delete(key: Key, to: SeqNr = SeqNr.Max): F[Option[PartitionOffset]]
}

object Journal {

  def empty[F[_] : Applicative]: Journal[F] = new Journal[F] {

    def append(key: Key, events: Nel[Event], metadata: Option[JsValue], headers: Headers) = PartitionOffset.Empty.pure[F]

    def read(key: Key, from: SeqNr) = Stream.empty

    def pointer(key: Key) = none[SeqNr].pure[F]

    def delete(key: Key, to: SeqNr) = none[PartitionOffset].pure[F]
  }

  
  def of[F[_] : Concurrent : ContextShift : Timer : Par : LogOf : KafkaConsumerOf : KafkaProducerOf : HeadCacheOf : RandomId](
    config: JournalConfig,
    origin: Option[Origin],
    eventualJournal: EventualJournal[F],
    metrics: Option[Metrics[F]]
  ): Resource[F, Journal[F]] = {

    val consumer = Consumer.of[F](config.consumer, config.pollTimeout)

    val headCache = {
      if (config.headCache) {
        HeadCacheOf[F].apply(config.consumer, eventualJournal)
      } else {
        Resource.pure[F, HeadCache[F]](HeadCache.empty[F])
      }
    }

    for {
      producer  <- Producer.of[F](config.producer)
      log       <- Resource.liftF(LogOf[F].apply(Journal.getClass))
      headCache <- headCache
    } yield {
      implicit val log1 = log
      val journal = apply(
        origin,
        producer,
        consumer,
        eventualJournal,
        headCache)
      val withLog = journal.withLog(log)
      metrics.fold(withLog) { metrics => withLog.withMetrics(metrics) }
    }
  }

  
  def apply[F[_] : Concurrent : ContextShift : Par : Clock : Log : RandomId](
    origin: Option[Origin],
    producer: Producer[F],
    consumer: Resource[F, Consumer[F]],
    eventualJournal: EventualJournal[F],
    headCache: HeadCache[F]
  ): Journal[F] = {

    val readActionsOf = ReadActionsOf[F](consumer)
    val appendAction = AppendAction[F](producer)
    apply[F](origin, eventualJournal, readActionsOf, appendAction, headCache)
  }


  def apply[F[_] : Concurrent : Log : Clock : Par : RandomId](
    origin: Option[Origin],
    eventual: EventualJournal[F],
    readActionsOf: ReadActionsOf[F],
    appendAction: AppendAction[F],
    headCache: HeadCache[F]
  ): Journal[F] = {

    def readActions(key: Key, from: SeqNr): F[(F[JournalInfo], FoldActions[F])] = {
      val marker = for {
        id              <- RandomId[F].get
        timestamp       <- Clock[F].instant
        action           = Action.Mark(key, timestamp, id, origin)
        partitionOffset <- appendAction(action)
        _               <- Log[F].debug(s"$key mark, id: $id, offset $partitionOffset")
      } yield {
        Marker(id, partitionOffset)
      }

      for {
        pointers <- Concurrent[F].start { eventual.pointers(key.topic) }
        marker   <- marker
        result   <- {
          if (marker.offset == Offset.Min) {
            for {
              _ <- pointers.cancel
            } yield {
              (JournalInfo.empty.pure[F], FoldActions.empty[F])
            }
          } else {
            for {
              result   <- headCache.get(key, partition = marker.partition, offset = Offset.Min max marker.offset - 1)
              pointers <- pointers.join
              offset    = pointers.values.get(marker.partition)
            } yield {

              val readKafka = FoldActions(key, from, marker, offset, readActionsOf)

              val info = result match {
                case HeadCache.Result.Valid(info) => info.pure[F]
                case HeadCache.Result.Invalid     =>
                  readKafka(None).fold(JournalInfo.empty) { (info, action) => info(action.action.header) }
              }
              (info, readKafka)
            }
          }
        }
      } yield result
    }

    new Journal[F] {

      def append(key: Key, events: Nel[Event], metadata: Option[JsValue], headers: Headers) = {
        for {
          timestamp <- Clock[F].instant
          metadata1  = Metadata(data = metadata)
          action     = Action.Append(key, timestamp, origin, events, metadata1, headers) // TODO measure
          result    <- appendAction(action)
        } yield result
      }

      def read(key: Key, from: SeqNr) = new Stream[F, EventRecord] {

        def foldWhileM[L, R](l: L)(f: (L, EventRecord) => F[Either[L, R]]) = {

          // TODO why do we need this?
          def replicatedSeqNr(from: SeqNr) = {
            val ss = (l, from.some, none[Offset])
            eventual.read(key, from).foldWhileM(ss) { case ((l, _, _), record) =>
              val event = record.event
              val offset = record.partitionOffset.offset
              val from = event.seqNr.next
              f(l, record).map {
                case Left(l)  => (l, from, offset.some).asLeft[(R, Option[SeqNr], Option[Offset])]
                case Right(r) => (r, from, offset.some).asRight[(L, Option[SeqNr], Option[Offset])]
              }
            }
          }

          def replicated(from: SeqNr) = {
            val events = eventual.read(key, from)
            events.foldWhileM(l)(f)
          }

          def onNonEmpty(deleteTo: Option[SeqNr], readKafka: FoldActions[F]) = {

            def events(from: SeqNr, offset: Option[Offset], l: L) = {
              readKafka(offset).foldWhileM(l) { (l, record) =>
                record.action match {
                  case _: Action.Delete => l.asLeft[R].pure[F]
                  case a: Action.Append =>
                    if (a.range.to < from) l.asLeft[R].pure[F]
                    else {
                      val events = EventsFromPayload(a.payload, a.payloadType)
                      events.foldWhileM(l) { case (l, event) =>
                        if (event.seqNr >= from) {
                          val eventRecord = EventRecord(a, event, record.partitionOffset)
                          f(l, eventRecord)
                        } else {
                          l.asLeft[R].pure[F]
                        }
                      }
                    }
                }
              }
            }

            val fromFixed = deleteTo.fold(from) { deleteTo => from max deleteTo.next }

            for {
              abc                    <- replicatedSeqNr(fromFixed)
              (result, from, offset)  = abc match {
                case Left((l, from, offset))  => (l.asLeft[R], from, offset)
                case Right((r, from, offset)) => (r.asRight[L], from, offset)
              }
              _                      <- Log[F].debug(s"$key read from: $from, offset: $offset")
              result                 <- from match {
                case None       => result.pure[F]
                case Some(from) => result match {
                  case Left(l)  => events(from, offset, l)
                  case Right(r) => r.asRight[L].pure[F]
                }
              }
            } yield result
          }

          for {
            ab                <- readActions(key, from)
            (info, readKafka)  = ab
            // TODO use range after eventualRecords
            // TODO prevent from reading calling consume twice!
            info              <- info
            _                 <- Log[F].debug(s"$key read info: $info")
            result            <- info match {
              case JournalInfo.Empty               => replicated(from)
              case JournalInfo.Append(_, deleteTo) => onNonEmpty(deleteTo, readKafka)
              // TODO test this case
              case JournalInfo.Delete(deleteTo) => deleteTo.next match {
                case None       => l.asLeft[R].pure[F]
                case Some(next) => replicated(from max next)
              }
            }
          } yield result
        }
      }

      def pointer(key: Key) = {
        // TODO reimplement, we don't need to call `eventual.pointer` without using it's offset

        val from = SeqNr.Min // TODO remove

        for {
          ab              <- readActions(key, from)
          (info, _)        = ab
          pointer          = Concurrent[F].start { eventual.pointer(key) }
          ab              <- Par[F].tupleN(info, pointer)
          (info, pointer)  = ab
          seqNrEventual    = for {
            pointer <- pointer.join
          } yield for {
            pointer <- pointer
          } yield {
            pointer.seqNr
          }
          seqNr           <- info match {
            case JournalInfo.Empty        => seqNrEventual
            case info: JournalInfo.Append => info.seqNr.some.pure[F]
            case _: JournalInfo.Delete    => seqNrEventual
          }
        } yield seqNr
      }

      def delete(key: Key, to: SeqNr) = {
        for {
          seqNr  <- pointer(key)
          result <- seqNr match {
            case None        => none[PartitionOffset].pure[F]
            case Some(seqNr) =>
              // TODO not delete already deleted, do not accept deleteTo=2 when already deleteTo=3
              val deleteTo = seqNr min to
              for {
                timestamp <- Clock[F].instant
                action     = Action.Delete(key, timestamp, deleteTo, origin)
                result    <- appendAction(action)
              } yield {
                result.some
              }
          }
        } yield result
      }
    }
  }


  def apply[F[_] : FlatMap : Clock](journal: Journal[F], log: Log[F]): Journal[F] = {

    val functionKId = FunctionK.id[F]

    new Journal[F] {

      def append(key: Key, events: Nel[Event], metadata: Option[JsValue], headers: Headers) = {
        for {
          rl     <- Latency { journal.append(key, events, metadata, headers) }
          (r, l)  = rl
          _      <- log.debug {
            val first = events.head.seqNr
            val last = events.last.seqNr
            val seqNr = if (first == last) s"seqNr: $first" else s"seqNrs: $first..$last"
            s"$key append in ${ l }ms, $seqNr, result: $r"
          }
        } yield r
      }

      def read(key: Key, from: SeqNr) = {
        val logging = new (F ~> F) {
          def apply[A](fa: F[A]) = {
            for {
              rl     <- Latency { fa }
              (r, l)  = rl
              _      <- log.debug(s"$key read in ${ l }ms, from: $from, result: $r")
            } yield r
          }
        }
        journal.read(key, from).mapK(logging, functionKId)
      }

      def pointer(key: Key) = {
        for {
          rl     <- Latency { journal.pointer(key) }
          (r, l)  = rl
          _      <- log.debug(s"$key pointer in ${ l }ms, result: $r")
        } yield r
      }

      def delete(key: Key, to: SeqNr) = {
        for {
          rl     <- Latency { journal.delete(key, to) }
          (r, l)  = rl
          _      <- log.debug(s"$key delete in ${ l }ms, to: $to, r: $r")
        } yield r
      }
    }
  }


  def apply[F[_] : Sync : Clock](journal: Journal[F], metrics: Metrics[F]): Journal[F] = {

    val functionKId = FunctionK.id[F]

    def latency[A](name: String, topic: Topic)(fa: F[A]): F[(A, Long)] = {
      Latency {
        fa.handleErrorWith { e =>
          for {
            _ <- metrics.failure(name, topic)
            a <- e.raiseError[F, A]
          } yield a
        }
      }
    }

    new Journal[F] {

      def append(key: Key, events: Nel[Event], metadata: Option[JsValue], headers: Headers) = {
        for {
          rl     <- latency("append", key.topic) { journal.append(key, events, metadata, headers) }
          (r, l)  = rl
          _      <- metrics.append(topic = key.topic, latency = l, events = events.size)
        } yield r
      }

      def read(key: Key, from: SeqNr) = {
        val measure = new (F ~> F) {
          def apply[A](fa: F[A]) = {
            for {
              rl     <- latency("read", key.topic) { fa }
              (r, l)  = rl
              _      <- metrics.read(topic = key.topic, latency = l)
            } yield r
          }
        }

        for {
          a <- journal.read(key, from).mapK(measure, functionKId)
          _ <- Stream.lift(metrics.read(key.topic))
        } yield a
      }

      def pointer(key: Key) = {
        for {
          rl     <- latency("pointer", key.topic) { journal.pointer(key) }
          (r, l)  = rl
          _      <- metrics.pointer(key.topic, l)
        } yield r
      }

      def delete(key: Key, to: SeqNr) = {
        for {
          rl     <- latency("delete", key.topic) { journal.delete(key, to) }
          (r, l)  = rl
          _      <- metrics.delete(key.topic, l)
        } yield r
      }
    }
  }


  trait Metrics[F[_]] {

    def append(topic: Topic, latency: Long, events: Int): F[Unit]

    def read(topic: Topic, latency: Long): F[Unit]

    def read(topic: Topic): F[Unit]

    def pointer(topic: Topic, latency: Long): F[Unit]

    def delete(topic: Topic, latency: Long): F[Unit]

    def failure(name: String, topic: Topic): F[Unit]
  }

  object Metrics {

    def const[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def append(topic: Topic, latency: Long, events: Int) = unit

      def read(topic: Topic, latency: Long) = unit

      def read(topic: Topic) = unit

      def pointer(topic: Topic, latency: Long) = unit

      def delete(topic: Topic, latency: Long) = unit

      def failure(name: String, topic: Topic) = unit
    }

    def empty[F[_] : Applicative]: Metrics[F] = const(().pure[F])
  }


  trait Producer[F[_]] {
    def send(record: ProducerRecord[Id, Bytes]): F[PartitionOffset]
  }

  object Producer {

    def of[F[_] : Sync : KafkaProducerOf](config: ProducerConfig): Resource[F, Producer[F]] = {

      val acks = config.acks match {
        case Acks.None => Acks.One
        case acks      => acks
      }

      val config1 = config.copy(
        acks = acks,
        idempotence = true,
        retries = config.retries max 10,
        common = config.common.copy(
          clientId = Some(config.common.clientId getOrElse "journal"),
          sendBufferBytes = config.common.sendBufferBytes max 1000000))

      for {
        kafkaProducer <- KafkaProducerOf[F].apply(config1)
      } yield {
        apply(kafkaProducer)
      }
    }

    def apply[F[_] : Sync](producer: KafkaProducer[F]): Producer[F] = {
      new Producer[F] {
        def send(record: ProducerRecord[Id, Bytes]) = {
          for {
            metadata  <- producer.send(record)
            partition  = metadata.topicPartition.partition
            offset    <- metadata.offset.fold {
              val error = new IllegalArgumentException("metadata.offset is missing, make sure ProducerConfig.acks set to One or All")
              error.raiseError[F, Offset]
            } {
              _.pure[F]
            }
          } yield {
            PartitionOffset(partition, offset)
          }
        }
      }
    }
  }


  trait Consumer[F[_]] {

    def assign(partitions: Nel[TopicPartition]): F[Unit]

    def seek(partition: TopicPartition, offset: Offset): F[Unit]

    def poll: F[ConsumerRecords[Id, Bytes]]
  }

  object Consumer {

    def of[F[_] : Sync : KafkaConsumerOf](
      config: ConsumerConfig,
      pollTimeout: FiniteDuration
    ): Resource[F, Consumer[F]] = {

      val config1 = config.copy(
        groupId = None,
        autoCommit = false)

      for {
        kafkaConsumer <- KafkaConsumerOf[F].apply[Id, Bytes](config1)
      } yield {
        apply[F](kafkaConsumer, pollTimeout)
      }
    }

    def apply[F[_]](
      consumer: KafkaConsumer[F, Id, Bytes],
      pollTimeout: FiniteDuration
    ): Consumer[F] = new Consumer[F] {

      def assign(partitions: Nel[TopicPartition]) = {
        consumer.assign(partitions)
      }

      def seek(partition: TopicPartition, offset: Offset) = {
        consumer.seek(partition, offset)
      }

      def poll = {
        consumer.poll(pollTimeout)
      }
    }
  }


  implicit class JournalOps[F[_]](val self: Journal[F]) extends AnyVal {

    def withLog(log: Log[F])(implicit F: FlatMap[F], clock: Clock[F]): Journal[F] =  {
      Journal(self, log)
    }


    def withLogError(log: Log[F])(implicit F: Sync[F], clock: Clock[F]): Journal[F] = {

      val functionKId = FunctionK.id[F]

      def logError[A](fa: F[A])(f: (Throwable, Long) => String) = {
        for {
          start  <- Clock[F].millis
          result <- fa.handleErrorWith { error =>
            for {
              end     <- Clock[F].millis
              latency  = end - start
              _       <- log.error(f(error, latency), error)
              result  <- error.raiseError[F, A]
            } yield result
          }
        } yield result
      }

      new Journal[F] {

        def append(key: Key, events: Nel[Event], metadata: Option[JsValue], headers: Headers) = {
          logError {
            self.append(key, events, metadata, headers)
          } { (error, latency) =>
            s"$key append failed in ${ latency }ms, events: $events, error: $error"
          }
        }

        def read(key: Key, from: SeqNr) = {
          val logging = new (F ~> F) {
            def apply[A](fa: F[A]) = {
              logError(fa) { (error, latency) =>
                s"$key read failed in ${ latency }ms, from: $from, error: $error"
              }
            }
          }
          self.read(key, from).mapK(logging, functionKId)
        }

        def pointer(key: Key) = {
          logError {
            self.pointer(key)
          } { (error, latency) =>
            s"$key pointer failed in ${ latency }ms, error: $error"
          }
        }

        def delete(key: Key, to: SeqNr) = {
          logError {
            self.delete(key, to)
          } { (error, latency) =>
            s"$key delete failed in ${ latency }ms, to: $to, error: $error"
          }
        }
      }
    }

    
    def withMetrics(metrics: Metrics[F])(implicit F: Sync[F], clock: Clock[F]): Journal[F] =  {
      Journal(self, metrics)
    }


    def mapK[G[_]](to: F ~> G, from: G ~> F): Journal[G] = new Journal[G] {

      def append(key: Key, events: Nel[Event], metadata: Option[JsValue], headers: Headers) = {
        to(self.append(key, events, metadata, headers))
      }

      def read(key: Key, from1: SeqNr) = {
        self.read(key, from1).mapK(to, from)
      }

      def pointer(key: Key) = {
        to(self.pointer(key))
      }

      def delete(key: Key, to1: SeqNr) = {
        to(self.delete(key, to1))
      }
    }
  }
}