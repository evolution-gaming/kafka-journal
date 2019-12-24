package com.evolutiongaming.kafka.journal

import cats._
import cats.arrow.FunctionK
import cats.data.{NonEmptyList => Nel}
import cats.effect._
import cats.implicits._
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf, MonadThrowable}
import com.evolutiongaming.kafka.journal.conversions.{EventsToPayload, PayloadToEvents}
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.Fail.implicits._
import com.evolutiongaming.kafka.journal.util.StreamHelper._
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.producer.{Acks, ProducerConfig, ProducerRecord}
import com.evolutiongaming.skafka.{Bytes => _, _}
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics._
import com.evolutiongaming.sstream.Stream
import play.api.libs.json.JsValue
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import scodec.bits.ByteVector

import scala.concurrent.duration._
import scala.util.Try

trait Journals[F[_]] {

  /**
   * @param expireAfter Define expireAfter in order to expire whole journal for given entity
   */
  def append(
    key: Key,
    events: Nel[Event],
    expireAfter: Option[ExpireAfter] = None, // TODO expiry: test
    metadata: Option[JsValue] = None,
    headers: Headers = Headers.empty
  ): F[PartitionOffset]

  // TODO expiry: make sure expireAfter is used during queries
  def read(key: Key, from: SeqNr = SeqNr.min): Stream[F, EventRecord]

  // TODO return Pointer and test it
  def pointer(key: Key): F[Option[SeqNr]]

  /**
   * Deletes events up to provided SeqNr
   */
  // TODO return Pointer and test it
  def delete(key: Key, to: DeleteTo = DeleteTo.max): F[Option[PartitionOffset]]

  /**
   * Deletes all data with regards to to, consecutive pointer call will return none
   */
  // TODO expiry: test
  def purge(key: Key): F[Option[PartitionOffset]]
}

object Journals {

  def empty[F[_] : Applicative]: Journals[F] = new Journals[F] {

    def append(
      key: Key,
      events: Nel[Event],
      expireAfter: Option[ExpireAfter],
      metadata: Option[JsValue],
      headers: Headers
    ) = PartitionOffset.empty.pure[F]

    def read(key: Key, from: SeqNr) = Stream.empty

    def pointer(key: Key) = none[SeqNr].pure[F]

    def delete(key: Key, to: DeleteTo) = none[PartitionOffset].pure[F]

    def purge(key: Key) = none[PartitionOffset].pure[F]
  }


  def of[F[_] : Concurrent : Timer : Parallel : LogOf : KafkaConsumerOf : KafkaProducerOf : HeadCacheOf : RandomIdOf : MeasureDuration : FromTry : Fail](
    config: JournalConfig,
    origin: Option[Origin],
    eventualJournal: EventualJournal[F],
    metrics: Option[Metrics[F]],
    callTimeThresholds: CallTimeThresholds
  ): Resource[F, Journals[F]] = {

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
      log       <- Resource.liftF(LogOf[F].apply(Journals.getClass))
      headCache <- headCache
    } yield {
      val journal = apply(
        origin,
        producer,
        consumer,
        eventualJournal,
        headCache,
        log)
      val withLog = journal.withLog(log, callTimeThresholds)
      metrics.fold(withLog) { metrics => withLog.withMetrics(metrics) }
    }
  }


  def apply[F[_] : Concurrent : Parallel : Clock : RandomIdOf : FromTry : Fail](
    origin: Option[Origin],
    producer: Producer[F],
    consumer: Resource[F, Consumer[F]],
    eventualJournal: EventualJournal[F],
    headCache: HeadCache[F],
    log: Log[F]
  ): Journals[F] = {
    implicit val fromAttempt = FromAttempt.lift[F]
    implicit val fromJsResult = FromJsResult.lift[F]

    apply[F](
      origin = origin,
      eventual = eventualJournal,
      consumeActionRecords = ConsumeActionRecords[F](consumer, log),
      appendAction = AppendAction[F](producer),
      headCache = headCache,
      payloadToEvents = PayloadToEvents[F],
      eventsToPayload = EventsToPayload[F],
      log = log)
  }


  def apply[F[_] : Concurrent : Clock : Parallel : RandomIdOf : FromTry](
    origin: Option[Origin],
    eventual: EventualJournal[F],
    consumeActionRecords: ConsumeActionRecords[F],
    appendAction: AppendAction[F],
    headCache: HeadCache[F],
    payloadToEvents: PayloadToEvents[F],
    eventsToPayload: EventsToPayload[F],
    log: Log[F]
  ): Journals[F] = {

    val appendMarker = AppendMarker(appendAction, origin)

    implicit val eventsToPayload1 = eventsToPayload
    val appendEvents = AppendEvents(appendAction, origin)

    def headAndStream(key: Key, from: SeqNr): F[(HeadInfo, F[StreamActionRecords[F]])] = {

      def headAndStream(marker: Marker) = {

        val stream = for {
          pointers <- eventual.pointers(key.topic)
        } yield {
          val offset = pointers.values.get(marker.partition)
          StreamActionRecords(key, from, marker, offset, consumeActionRecords)
        }

        val offset = marker
          .offset
          .dec[Try]
          .getOrElse(Offset.min)

        for {
          result <- headCache.get(key, partition = marker.partition, offset = offset)
          result <- result match {
            case Right(headInfo) => (headInfo, stream).pure[F]
            case Left(_)         =>
              for {
                stream   <- stream
                headInfo <- stream(none).fold(HeadInfo.empty) { (info, action) => info(action.action.header, action.offset) }
              } yield {
                (headInfo, stream.pure[F])
              }
          }
        } yield result
      }

      for {
        marker   <- appendMarker(key)
        result   <- {
          if (marker.offset === Offset.min) {
            (HeadInfo.empty, StreamActionRecords.empty[F].pure[F]).pure[F]
          } else {
            headAndStream(marker)
          }
        }
      } yield result
    }

    new Journals[F] {

      def append(
        key: Key,
        events: Nel[Event],
        expireAfter: Option[ExpireAfter],
        metadata: Option[JsValue],
        headers: Headers
      ) = {
        appendEvents(key, events, expireAfter, metadata, headers)
      }


      def read(key: Key, from: SeqNr) = {

        def readEventualAndKafka(from: SeqNr, stream: F[StreamActionRecords[F]], offsetAppend: Offset) = {

          def readKafka(from: SeqNr, offset: Option[Offset], stream: StreamActionRecords[F]) = {

            val appends = stream(offset)
              .collect { case a@ActionRecord(_: Action.Append, _) => a.asInstanceOf[ActionRecord[Action.Append]] }
              .dropWhile { a => a.action.range.to < from }

            for {
              record         <- appends
              action          = record.action
              payloadAndType  = PayloadAndType(action)
              events         <- Stream.lift(payloadToEvents(payloadAndType))
              event          <- Stream[F].apply(events.events)
              if event.seqNr >= from
            } yield {
              EventRecord(action, event, record.partitionOffset)
            }
          }

          for {
            stream <- Stream.lift(stream)
            event  <- eventual
              .read(key, from)
              .flatMapLast { last =>
                last
                  .fold((from, none[Offset]).some) { event =>
                    event.seqNr.next[Option].map { from => (from, event.offset.some) }
                  }
                  .fold(Stream.empty[F, EventRecord]) { case (from, offsetRecord) =>
                    val offset = offsetAppend.dec[Try].toOption max offsetRecord // TODO refactor this, pass from offset, rather than from - 1
                    readKafka(from, offset, stream)
                  }
            }
          } yield event
        }

        def read(head: HeadInfo, stream: F[StreamActionRecords[F]]) = {

          def empty = Stream.empty[F, EventRecord]

          def readEventual(from: SeqNr) = eventual.read(key, from)

          def onAppend(offset: Offset, deleteTo: Option[DeleteTo]) = {

            def readEventualAndKafka1(from: SeqNr) = readEventualAndKafka(from, stream, offset)

            deleteTo.fold {
              readEventualAndKafka1(from)
            } { deleteTo =>
              deleteTo
                .value
                .next[Option]
                .fold(empty) { min => readEventualAndKafka1(min max from) }
            }
          }

          def onDelete(deleteTo: DeleteTo) = {
            deleteTo
              .value
              .next[Option]
              .fold(empty) { min => readEventual(min max from) }
          }

          head match {
            case HeadInfo.Empty     => readEventual(from)
            case a: HeadInfo.Append => onAppend(a.offset, a.deleteTo)
            case a: HeadInfo.Delete => onDelete(a.deleteTo)
            case HeadInfo.Purge     => empty
          }
        }

        for {
          headAndStream  <- Stream.lift(headAndStream(key, from))
          (head, stream)  = headAndStream
          _              <- Stream.lift(log.debug(s"$key read info: $head"))
          eventRecord    <- read(head, stream)
        } yield eventRecord
      }


      def pointer(key: Key) = {

        // TODO refactor, we don't need to call `eventual.pointer` without using it's offset
        def pointerEventual = for {
          pointer <- eventual.pointer(key)
        } yield for {
          pointer <- pointer
        } yield {
          pointer.seqNr
        }

        def pointer(headInfo: HeadInfo) = {
          headInfo match {
            case HeadInfo.Empty     => pointerEventual
            case a: HeadInfo.Append => a.seqNr.some.pure[F]
            case _: HeadInfo.Delete => pointerEventual
            case HeadInfo.Purge     => none[SeqNr].pure[F]
          }
        }

        val from = SeqNr.min // TODO remove

        for {
          headAndStream <- headAndStream(key, from)
          (headInfo, _)  = headAndStream
          pointer       <- pointer(headInfo)
        } yield pointer
      }

      // TODO not delete already deleted, do not accept deleteTo=2 when already deleteTo=3
      def delete(key: Key, to: DeleteTo) = {

        def delete(to: DeleteTo) = {
          for {
            timestamp <- Clock[F].instant
            action     = Action.Delete(key, timestamp, to, origin)
            result    <- appendAction(action)
          } yield result
        }

        for {
          seqNr   <- pointer(key)
          pointer <- seqNr.traverse { seqNr => delete(seqNr.toDeleteTo min to) }
        } yield pointer
      }

      def purge(key: Key): F[Option[PartitionOffset]] = {
        // TODO expiry: move usages of Action.scala from Journal.scala to AppendAction
        for {
          timestamp <- Clock[F].instant
          action     = Action.Purge(key, timestamp, origin)
          result    <- appendAction(action)
        } yield {
          result.some
        }
      }
    }
  }


  trait Metrics[F[_]] {

    def append(topic: Topic, latency: FiniteDuration, events: Int): F[Unit]

    def read(topic: Topic, latency: FiniteDuration): F[Unit]

    def read(topic: Topic): F[Unit]

    def pointer(topic: Topic, latency: FiniteDuration): F[Unit]

    def delete(topic: Topic, latency: FiniteDuration): F[Unit]

    def purge(topic: Topic, latency: FiniteDuration): F[Unit]

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

      def purge(topic: Topic, latency: FiniteDuration) = unit

      def failure(name: String, topic: Topic) = unit
    }


    def of[F[_] : Monad](
      registry: CollectorRegistry[F],
      prefix: String = "journal"
    ): Resource[F, Journals.Metrics[F]] = {

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

        new Journals.Metrics[F] {

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

          def purge(topic: Topic, latency: FiniteDuration) = {
            observeLatency(name = "purge", topic = topic, latency = latency)
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

    def of[F[_] : Monad : KafkaProducerOf : FromTry : Fail](config: ProducerConfig): Resource[F, Producer[F]] = {

      val acks = config.acks match {
        case Acks.None => Acks.One
        case acks      => acks
      }

      val config1 = config.copy(
        acks = acks,
        idempotence = true,
        retries = config.retries max 10,
        common = config.common.copy(
          clientId = config.common.clientId.getOrElse("journal").some,
          sendBufferBytes = config.common.sendBufferBytes max 1000000))

      for {
        kafkaProducer <- KafkaProducerOf[F].apply(config1)
      } yield {
        import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
        apply(kafkaProducer)
      }
    }

    def apply[F[_] : Monad : FromTry : Fail](
      producer: KafkaProducer[F]
    )(implicit
      toBytesKey: skafka.ToBytes[F, String],
      toBytesValue: skafka.ToBytes[F, ByteVector],
    ): Producer[F] = {
      record: ProducerRecord[String, ByteVector] => {
        for {
          metadata  <- producer.send(record)(toBytesKey, toBytesValue)
          partition  = metadata.topicPartition.partition
          offset    <- metadata.offset.fold {
            "metadata.offset is missing, make sure ProducerConfig.acks set to One or All".fail[F, Offset]
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

    def poll: F[ConsRecords]
  }

  object Consumer {

    def of[F[_] : MonadThrowable : KafkaConsumerOf : FromTry](
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


  implicit class JournalOps[F[_]](val self: Journals[F]) extends AnyVal {

    def withLog(
      log: Log[F],
      config: CallTimeThresholds = CallTimeThresholds.default)(implicit
      F: FlatMap[F],
      measureDuration: MeasureDuration[F]
    ): Journals[F] = {

      val functionKId = FunctionK.id[F]

      def logDebugOrWarn(latency: FiniteDuration, threshold: FiniteDuration)(msg: => String) = {
        if (latency >= threshold) log.warn(msg) else log.debug(msg)
      }

      new Journals[F] {

        def append(
          key: Key,
          events: Nel[Event],
          expireAfter: Option[ExpireAfter],
          metadata: Option[JsValue],
          headers: Headers
        ) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.append(key, events, expireAfter, metadata, headers)
            d <- d
            _ <- logDebugOrWarn(d, config.append) {
              val first = events.head.seqNr
              val last = events.last.seqNr
              val seqNr = if (first === last) s"seqNr: $first" else s"seqNrs: $first..$last"
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

        def delete(key: Key, to: DeleteTo) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.delete(key, to)
            d <- d
            _ <- logDebugOrWarn(d, config.delete) { s"$key delete in ${ d.toMillis }ms, to: $to, result: $r" }
          } yield r
        }

        def purge(key: Key) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.purge(key)
            d <- d
            _ <- logDebugOrWarn(d, config.purge) { s"$key purge in ${ d.toMillis }ms, result: $r" }
          } yield r
        }
      }
    }


    def withLogError(log: Log[F])(implicit F: MonadThrowable[F], measureDuration: MeasureDuration[F]): Journals[F] = {

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

      new Journals[F] {

        def append(
          key: Key,
          events: Nel[Event],
          expireAfter: Option[ExpireAfter],
          metadata: Option[JsValue],
          headers: Headers
        ) = {
          logError {
            self.append(key, events, expireAfter, metadata, headers)
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

        def delete(key: Key, to: DeleteTo) = {
          logError {
            self.delete(key, to)
          } { (error, latency) =>
            s"$key delete failed in ${ latency.toMillis }ms, to: $to, error: $error"
          }
        }

        def purge(key: Key) = {
          logError {
            self.purge(key)
          } { (error, latency) =>
            s"$key purge failed in ${ latency.toMillis }ms, error: $error"
          }
        }
      }
    }


    def withMetrics(
      metrics: Metrics[F])(implicit
      F: MonadThrowable[F],
      measureDuration: MeasureDuration[F]
    ): Journals[F] = {
      val functionKId = FunctionK.id[F]

      def handleError[A](name: String, topic: Topic)(fa: F[A]): F[A] = {
        fa.handleErrorWith { e =>
          for {
            _ <- metrics.failure(name, topic)
            a <- e.raiseError[F, A]
          } yield a
        }
      }

      new Journals[F] {

        def append(
          key: Key,
          events: Nel[Event],
          expireAfter: Option[ExpireAfter],
          metadata: Option[JsValue],
          headers: Headers
        ) = {
          def append = self.append(key, events, expireAfter, metadata, headers)
          for {
            d <- MeasureDuration[F].start
            r <- handleError("append", key.topic) { append }
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
            a <- self.read(key, from).mapK(measure, functionKId)
            _ <- Stream.lift(metrics.read(key.topic))
          } yield a
        }

        def pointer(key: Key) = {
          for {
            d <- MeasureDuration[F].start
            r <- handleError("pointer", key.topic) { self.pointer(key) }
            d <- d
            _ <- metrics.pointer(key.topic, d)
          } yield r
        }

        def delete(key: Key, to: DeleteTo) = {
          for {
            d <- MeasureDuration[F].start
            r <- handleError("delete", key.topic) { self.delete(key, to) }
            d <- d
            _ <- metrics.delete(key.topic, d)
          } yield r
        }

        def purge(key: Key) = {
          for {
            d <- MeasureDuration[F].start
            r <- handleError("purge", key.topic) { self.purge(key) }
            d <- d
            _ <- metrics.purge(key.topic, d)
          } yield r
        }
      }
    }


    def mapK[G[_]](fg: F ~> G, gf: G ~> F): Journals[G] = new Journals[G] {

      def append(
        key: Key,
        events: Nel[Event],
        expireAfter: Option[ExpireAfter],
        metadata: Option[JsValue],
        headers: Headers
      ) = {
        fg(self.append(key, events, expireAfter, metadata, headers))
      }

      def read(key: Key, from1: SeqNr) = self.read(key, from1).mapK(fg, gf)

      def pointer(key: Key) = fg(self.pointer(key))

      def delete(key: Key, to: DeleteTo) = fg(self.delete(key, to))

      def purge(key: Key) = fg(self.purge(key))
    }
  }


  final case class CallTimeThresholds(
    append: FiniteDuration = 500.millis,
    read: FiniteDuration = 5.seconds,
    pointer: FiniteDuration = 1.second,
    delete: FiniteDuration = 1.second,
    purge: FiniteDuration = 1.second)

  object CallTimeThresholds {

    val default: CallTimeThresholds = CallTimeThresholds()

    implicit val configReaderCallTimeThresholds: ConfigReader[CallTimeThresholds] = deriveReader[CallTimeThresholds]
  }
}