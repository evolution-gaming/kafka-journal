package com.evolutiongaming.kafka.journal

import cats._
import cats.arrow.FunctionK
import cats.data.{NonEmptyList => Nel}
import cats.effect._
import cats.implicits._
import cats.temp.par._
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.journal.conversions.{EventsToPayload, PayloadToEvents}
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerRecords}
import com.evolutiongaming.skafka.producer.{Acks, ProducerConfig, ProducerRecord}
import com.evolutiongaming.skafka.{Bytes => _, _}
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics._
import com.evolutiongaming.sstream.FoldWhile.FoldWhileOps
import com.evolutiongaming.sstream.Stream
import play.api.libs.json.JsValue
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import scodec.bits.ByteVector

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


  def of[F[_] : Concurrent : ContextShift : Timer : Parallel : LogOf : KafkaConsumerOf : KafkaProducerOf : HeadCacheOf : RandomId : MeasureDuration : FromTry](
    config: JournalConfig,
    origin: Option[Origin],
    eventualJournal: EventualJournal[F],
    metrics: Option[Metrics[F]],
    callTimeThresholds: CallTimeThresholds
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
      val withLog = journal.withLog(log, callTimeThresholds)
      metrics.fold(withLog) { metrics => withLog.withMetrics(metrics) }
    }
  }


  def apply[F[_] : Concurrent : ContextShift : Parallel : Clock : Log : RandomId : FromTry](
    origin: Option[Origin],
    producer: Producer[F],
    consumer: Resource[F, Consumer[F]],
    eventualJournal: EventualJournal[F],
    headCache: HeadCache[F]
  ): Journal[F] = {

    implicit val fromAttempt = FromAttempt.lift[F]
    implicit val fromJsResult = FromJsResult.lift[F]

    val readActionsOf = ReadActionsOf[F](consumer)
    val appendAction = AppendAction[F](producer)
    apply[F](origin, eventualJournal, readActionsOf, appendAction, headCache)
  }


  def apply[F[_] : Concurrent : Log : Clock : Parallel : RandomId : FromTry](
    origin: Option[Origin],
    eventual: EventualJournal[F],
    readActionsOf: ReadActionsOf[F],
    appendAction: AppendAction[F],
    headCache: HeadCache[F])(implicit
    payloadToEvents: PayloadToEvents[F],
    eventsToPayload: EventsToPayload[F]
  ): Journal[F] = {

    val appendMarker = AppendMarker(appendAction, origin)

    val appendEvents = AppendEvents(appendAction, origin)

    def readActions(key: Key, from: SeqNr): F[(F[JournalInfo], FoldActions[F])] = {
      for {
        pointers <- Concurrent[F].start { eventual.pointers(key.topic) }
        marker   <- appendMarker(key)
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
        appendEvents(key, events, metadata, headers)
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

                      def read(events: Nel[Event]) = {
                        events.foldWhileM(l) { case (l, event) =>
                          if (event.seqNr >= from) {
                            val eventRecord = EventRecord(a, event, record.partitionOffset)
                            f(l, eventRecord)
                          } else {
                            l.asLeft[R].pure[F]
                          }
                        }
                      }

                      val payloadAndType = PayloadAndType(a)

                      for {
                        events <- payloadToEvents(payloadAndType)
                        result <- read(events)
                      } yield result
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
          ab              <- (info, pointer).parTupled
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


  def apply[F[_] : Sync : MeasureDuration](journal: Journal[F], metrics: Metrics[F]): Journal[F] = {

    val functionKId = FunctionK.id[F]

    def handleError[A](name: String, topic: Topic)(fa: F[A]): F[A] = {
      fa.handleErrorWith { e =>
        for {
          _ <- metrics.failure(name, topic)
          a <- e.raiseError[F, A]
        } yield a
      }
    }

    new Journal[F] {

      def append(key: Key, events: Nel[Event], metadata: Option[JsValue], headers: Headers) = {
        for {
          d <- MeasureDuration[F].start
          r <- handleError("append", key.topic) { journal.append(key, events, metadata, headers) }
          d <- d
          _ <- metrics.append(topic = key.topic, latency = d, events = events.size)
        } yield r
      }

      def read(key: Key, from: SeqNr) = {
        val measure = new (F ~> F) {
          def apply[A](fa: F[A]) = {
            for {
              d <- MeasureDuration[F].start
              r <- handleError("read", key.topic) { fa }
              d <- d
              _ <- metrics.read(topic = key.topic, latency = d)
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
          d <- MeasureDuration[F].start
          r <- handleError("pointer", key.topic) { journal.pointer(key) }
          d <- d
          _ <- metrics.pointer(key.topic, d)
        } yield r
      }

      def delete(key: Key, to: SeqNr) = {
        for {
          d <- MeasureDuration[F].start
          r <- handleError("delete", key.topic) { journal.delete(key, to) }
          d <- d
          _ <- metrics.delete(key.topic, d)
        } yield r
      }
    }
  }


  trait Metrics[F[_]] {

    def append(topic: Topic, latency: FiniteDuration, events: Int): F[Unit]

    def read(topic: Topic, latency: FiniteDuration): F[Unit]

    def read(topic: Topic): F[Unit]

    def pointer(topic: Topic, latency: FiniteDuration): F[Unit]

    def delete(topic: Topic, latency: FiniteDuration): F[Unit]

    def failure(name: String, topic: Topic): F[Unit]
  }

  object Metrics {

    def empty[F[_] : Applicative]: Metrics[F] = const(().pure[F])
    

    def const[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def append(topic: Topic, latency: FiniteDuration, events: Int) = unit

      def read(topic: Topic, latency: FiniteDuration) = unit

      def read(topic: Topic) = unit

      def pointer(topic: Topic, latency: FiniteDuration) = unit

      def delete(topic: Topic, latency: FiniteDuration) = unit

      def failure(name: String, topic: Topic) = unit
    }


    def of[F[_] : Monad](
      registry: CollectorRegistry[F],
      prefix: String = "journal"
    ): Resource[F, Journal.Metrics[F]] = {

      val latencySummary = registry.summary(
        name = s"${ prefix }_topic_latency",
        help = "Journal call latency in seconds",
        quantiles = Quantiles(
          Quantile(0.9, 0.05),
          Quantile(0.99, 0.005)),
        labels = LabelNames("topic", "type"))

      val eventsSummary = registry.summary(
        name = s"${ prefix }_events",
        help = "Number of events",
        quantiles = Quantiles.Empty,
        labels = LabelNames("topic", "type"))

      val resultCounter = registry.counter(
        name = s"${ prefix }_results",
        help = "Call result: success or failure",
        labels = LabelNames("topic", "type", "result"))

      for {
        latencySummary <- latencySummary
        eventsSummary  <- eventsSummary
        resultCounter  <- resultCounter
      } yield {

        def observeLatency(name: String, topic: Topic, latency: FiniteDuration) = {
          for {
            _ <- latencySummary.labels(topic, name).observe(latency.toNanos.nanosToSeconds)
            _ <- resultCounter.labels(topic, name, "success").inc()
          } yield {}
        }

        def observeEvents(name: String, topic: Topic, events: Int) = {
          eventsSummary.labels(topic, name).observe(events.toDouble)
        }

        new Journal.Metrics[F] {

          def append(topic: Topic, latency: FiniteDuration, events: Int) = {
            for {
              _ <- observeEvents(name = "append", topic = topic, events = events)
              _ <- observeLatency(name = "append", topic = topic, latency = latency)
            } yield {}
          }

          def read(topic: Topic, latency: FiniteDuration) = {
            observeLatency(name = "read", topic = topic, latency = latency)
          }

          def read(topic: Topic) = {
            observeEvents(name = "read", topic = topic, events = 1)
          }

          def pointer(topic: Topic, latency: FiniteDuration) = {
            observeLatency(name = "pointer", topic = topic, latency = latency)
          }

          def delete(topic: Topic, latency: FiniteDuration) = {
            observeLatency(name = "delete", topic = topic, latency = latency)
          }

          def failure(name: String, topic: Topic) = {
            resultCounter.labels(topic, name, "failure").inc()
          }
        }
      }
    }
  }


  trait Producer[F[_]] {
    def send(record: ProducerRecord[String, ByteVector]): F[PartitionOffset]
  }

  object Producer {

    def of[F[_] : Sync : KafkaProducerOf : FromTry](config: ProducerConfig): Resource[F, Producer[F]] = {

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
        import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
        apply(kafkaProducer)
      }
    }

    def apply[F[_] : Sync : FromTry](
      producer: KafkaProducer[F]
    )(implicit
      toBytesKey: skafka.ToBytes[F, String],
      toBytesValue: skafka.ToBytes[F, ByteVector],
    ): Producer[F] = {
      record: ProducerRecord[String, ByteVector] => {
        for {
          metadata  <- producer.send(record)
          partition  = metadata.topicPartition.partition
          offset    <- metadata.offset.fold {
            val error = JournalError("metadata.offset is missing, make sure ProducerConfig.acks set to One or All")
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


  trait Consumer[F[_]] {

    def assign(partitions: Nel[TopicPartition]): F[Unit]

    def seek(partition: TopicPartition, offset: Offset): F[Unit]

    def poll: F[ConsumerRecords[String, ByteVector]]
  }

  object Consumer {

    def of[F[_] : Sync : KafkaConsumerOf : FromTry](
      config: ConsumerConfig,
      pollTimeout: FiniteDuration
    ): Resource[F, Consumer[F]] = {
      import com.evolutiongaming.kafka.journal.util.SkafkaHelper._

      val config1 = config.copy(
        groupId = None,
        autoCommit = false)

      for {
        kafkaConsumer <- KafkaConsumerOf[F].apply[String, ByteVector](config1)
      } yield {
        apply[F](kafkaConsumer, pollTimeout)
      }
    }

    def apply[F[_]](
      consumer: KafkaConsumer[F, String, ByteVector],
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

    def withLog(
      log: Log[F],
      config: CallTimeThresholds = CallTimeThresholds.Default)(implicit
      F: FlatMap[F],
      measureDuration: MeasureDuration[F]
    ): Journal[F] = {

      val functionKId = FunctionK.id[F]

      def logDebugOrWarn(latency: FiniteDuration, threshold: FiniteDuration)(msg: => String) = {
        if (latency >= threshold) log.warn(msg) else log.debug(msg)
      }

      new Journal[F] {

        def append(key: Key, events: Nel[Event], metadata: Option[JsValue], headers: Headers) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.append(key, events, metadata, headers)
            d <- d
            _ <- logDebugOrWarn(d, config.append) {
              val first = events.head.seqNr
              val last = events.last.seqNr
              val seqNr = if (first == last) s"seqNr: $first" else s"seqNrs: $first..$last"
              s"$key append in ${ d.toMillis }ms, $seqNr, result: $r"
            }
          } yield r
        }

        def read(key: Key, from: SeqNr) = {
          val logging = new (F ~> F) {
            def apply[A](fa: F[A]) = {
              for {
                d <- MeasureDuration[F].start
                r <- fa
                d <- d
                _ <- logDebugOrWarn(d, config.read) { s"$key read in ${ d.toMillis }ms, from: $from, result: $r" }
              } yield r
            }
          }
          self.read(key, from).mapK(logging, functionKId)
        }

        def pointer(key: Key) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.pointer(key)
            d <- d
            _ <- logDebugOrWarn(d, config.pointer) { s"$key pointer in ${ d.toMillis }ms, result: $r" }
          } yield r
        }

        def delete(key: Key, to: SeqNr) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.delete(key, to)
            d <- d
            _ <- logDebugOrWarn(d, config.delete) { s"$key delete in ${ d.toMillis }ms, to: $to, r: $r" }
          } yield r
        }
      }
    }


    def withLogError(log: Log[F])(implicit F: Sync[F], measureDuration: MeasureDuration[F]): Journal[F] = {

      val functionKId = FunctionK.id[F]

      def logError[A](fa: F[A])(f: (Throwable, FiniteDuration) => String) = {
        for {
          d <- MeasureDuration[F].start
          r <- fa.handleErrorWith { error =>
            for {
              d <- d
              _ <- log.error(f(error, d), error)
              r <- error.raiseError[F, A]
            } yield r
          }
        } yield r
      }

      new Journal[F] {

        def append(key: Key, events: Nel[Event], metadata: Option[JsValue], headers: Headers) = {
          logError {
            self.append(key, events, metadata, headers)
          } { (error, latency) =>
            s"$key append failed in ${ latency.toMillis }ms, events: $events, error: $error"
          }
        }

        def read(key: Key, from: SeqNr) = {
          val logging = new (F ~> F) {
            def apply[A](fa: F[A]) = {
              logError(fa) { (error, latency) =>
                s"$key read failed in ${ latency.toMillis }ms, from: $from, error: $error"
              }
            }
          }
          self.read(key, from).mapK(logging, functionKId)
        }

        def pointer(key: Key) = {
          logError {
            self.pointer(key)
          } { (error, latency) =>
            s"$key pointer failed in ${ latency.toMillis }ms, error: $error"
          }
        }

        def delete(key: Key, to: SeqNr) = {
          logError {
            self.delete(key, to)
          } { (error, latency) =>
            s"$key delete failed in ${ latency.toMillis }ms, to: $to, error: $error"
          }
        }
      }
    }


    def withMetrics(metrics: Metrics[F])(implicit F: Sync[F], measureDuration: MeasureDuration[F]): Journal[F] = {
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


  final case class CallTimeThresholds(
    append: FiniteDuration = 500.millis,
    read: FiniteDuration = 5.seconds,
    pointer: FiniteDuration = 1.second,
    delete: FiniteDuration = 1.second)

  object CallTimeThresholds {
    val Default: CallTimeThresholds = CallTimeThresholds()

    implicit val ConfigReaderVal: ConfigReader[CallTimeThresholds] = deriveReader[CallTimeThresholds]
  }
}