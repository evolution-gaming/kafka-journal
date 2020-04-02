package com.evolutiongaming.kafka.journal

import cats._
import cats.data.{NonEmptyList => Nel, NonEmptySet => Nes}
import cats.effect._
import cats.implicits._
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf, MonadThrowable, ToTry}
import com.evolutiongaming.kafka.journal.conversions.{ConversionMetrics, EventsToPayload, PayloadToEvents}
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.Fail.implicits._
import com.evolutiongaming.kafka.journal.util.StreamHelper._
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.producer.{Acks, ProducerConfig, ProducerRecord}
import com.evolutiongaming.skafka.{Bytes => _, _}
import com.evolutiongaming.smetrics._
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector

import scala.concurrent.duration._
import scala.util.Try

trait Journals[F[_]] {

  def apply(key: Key): Journal[F]
}

object Journals {

  def empty[F[_] : Applicative]: Journals[F] = const(Journal.empty[F])


  def const[F[_]](journal: Journal[F]): Journals[F] = (_: Key) => journal


  def of[
    F[_]
    : Concurrent : Parallel : Timer
    : FromTry : ToTry : Fail : LogOf
    : KafkaConsumerOf : KafkaProducerOf : HeadCacheOf : RandomIdOf
    : MeasureDuration
    : JsonCodec
  ](
    config: JournalConfig,
    origin: Option[Origin],
    eventualJournal: EventualJournal[F],
    journalMetrics: Option[JournalMetrics[F]],
    conversionMetrics: Option[ConversionMetrics[F]],
    callTimeThresholds: Journal.CallTimeThresholds
  ): Resource[F, Journals[F]] = {

    val consumer = Consumer.of[F](config.kafka.consumer, config.pollTimeout)

    val headCache = {
      if (config.headCache.enabled) {
        HeadCacheOf[F].apply(config.kafka.consumer, eventualJournal)
      } else {
        Resource.pure[F, HeadCache[F]](HeadCache.empty[F])
      }
    }

    for {
      producer  <- Producer.of[F](config.kafka.producer)
      log       <- Resource.liftF(LogOf[F].apply(Journals.getClass))
      headCache <- headCache
    } yield {
      val journal = apply(
        origin,
        producer,
        consumer,
        eventualJournal,
        headCache,
        log,
        conversionMetrics
      )
      val withLog = journal.withLog(log, callTimeThresholds)
      journalMetrics.fold(withLog) { metrics => withLog.withMetrics(metrics) }
    }
  }


  def apply[F[_] : Concurrent : Parallel : Clock : RandomIdOf : FromTry : ToTry : Fail : JsonCodec : MeasureDuration](
    origin: Option[Origin],
    producer: Producer[F],
    consumer: Resource[F, Consumer[F]],
    eventualJournal: EventualJournal[F],
    headCache: HeadCache[F],
    log: Log[F],
    conversionMetrics: Option[ConversionMetrics[F]]
  ): Journals[F] = {
    implicit val fromAttempt: FromAttempt[F]   = FromAttempt.lift[F]
    implicit val fromJsResult: FromJsResult[F] = FromJsResult.lift[F]
    implicit val jsonCodec: JsonCodec[Try]     = JsonCodec.summon[F].mapK(ToTry.functionK)

    val payloadToEvents = conversionMetrics.fold(PayloadToEvents[F]) { metrics =>
      PayloadToEvents[F].withMetrics(metrics.payloadToEvents)
    }

    val eventsToPayload = conversionMetrics.fold(EventsToPayload[F]) { metrics =>
      EventsToPayload[F].withMetrics(metrics.eventsToPayload)
    }

    apply[F](
      eventual = eventualJournal,
      consumeActionRecords = ConsumeActionRecords[F](consumer, log),
      produce = Produce[F](producer, origin),
      headCache = headCache,
      payloadToEvents = payloadToEvents,
      eventsToPayload = eventsToPayload,
      log = log)
  }


  def apply[F[_] : Concurrent : Clock : Parallel : RandomIdOf : FromTry](
    eventual: EventualJournal[F],
    consumeActionRecords: ConsumeActionRecords[F],
    produce: Produce[F],
    headCache: HeadCache[F],
    payloadToEvents: PayloadToEvents[F],
    eventsToPayload: EventsToPayload[F],
    log: Log[F]
  ): Journals[F] = {

    val appendMarker = AppendMarker(produce)
    implicit val eventsToPayload1 = eventsToPayload
    val appendEvents = AppendEvents(produce)

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

      def apply(key: Key) = new Journal[F] {

        def append(events: Nel[Event], metadata: RecordMetadata, headers: Headers) = {
          appendEvents(key, events, metadata, headers)
        }

        def read(from: SeqNr) = {

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
                EventRecord(action, event, record.partitionOffset, events.metadata)
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


        def pointer = {

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
        def delete(to: DeleteTo) = {
          for {
            seqNr   <- pointer
            pointer <- seqNr.traverse { seqNr => produce.delete(key, seqNr.toDeleteTo min to) }
          } yield pointer
        }

        def purge: F[Option[PartitionOffset]] = {
          produce
            .purge(key)
            .map { _.some }
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

    def assign(partitions: Nes[TopicPartition]): F[Unit]

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

      def assign(partitions: Nes[TopicPartition]) = {
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


  implicit class JournalsOps[F[_]](val self: Journals[F]) extends AnyVal {

    def withLog(
      log: Log[F],
      config: Journal.CallTimeThresholds = Journal.CallTimeThresholds.default)(implicit
      F: FlatMap[F],
      measureDuration: MeasureDuration[F]
    ): Journals[F] = {
      key: Key => self(key).withLog(key, log, config)
    }


    def withLogError(
      log: Log[F])(implicit
      F: MonadThrowable[F],
      measureDuration: MeasureDuration[F]
    ): Journals[F] = {
      key: Key => self(key).withLogError(key, log)
    }


    def withMetrics(
      metrics: JournalMetrics[F])(implicit
      F: MonadThrowable[F],
      measureDuration: MeasureDuration[F]
    ): Journals[F] = {
      key: Key => self(key).withMetrics(key.topic, metrics)
    }


    def mapK[G[_]](fg: F ~> G, gf: G ~> F): Journals[G] = {
      key: Key => self(key).mapK(fg, gf)
    }
  }
}