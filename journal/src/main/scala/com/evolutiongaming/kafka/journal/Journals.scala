package com.evolutiongaming.kafka.journal

import cats._
import cats.data.{NonEmptyList => Nel, NonEmptySet => Nes}
import cats.effect._
import cats.syntax.all._
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf, MeasureDuration, MonadThrowable}
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.kafka.journal.Journal.ConsumerPoolConfig
import com.evolutiongaming.kafka.journal.conversions.{ConversionMetrics, KafkaRead, KafkaWrite}
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, EventualRead}
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.Fail.implicits._
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.kafka.journal.util.StreamHelper._
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.producer.{Acks, ProducerConfig, ProducerRecord}
import com.evolutiongaming.skafka.{Bytes => _, _}
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector

import scala.concurrent.duration._
import scala.util.Try

trait Journals[F[_]] {

  def apply(key: Key): Journal[F]
}

object Journals {

  def empty[F[_] : Applicative]: Journals[F] = const(Journal.empty[F])


  def const[F[_]](journal: Journal[F]): Journals[F] = {
    class Const
    new Const with Journals[F] {
      def apply(key: Key) = journal
    }
  }

  @deprecated("use `of1`", "2023-07-26")
  def of[
    F[_]
    : Concurrent : Timer
    : FromTry : Fail : LogOf
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
      log       <- LogOf[F].apply(Journals.getClass).toResource
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

  def of1[
    F[_]
    : Concurrent: Timer: Parallel
    : FromTry: Fail: LogOf
    : KafkaConsumerOf : KafkaProducerOf : HeadCacheOf : RandomIdOf
    : MeasureDuration
    : JsonCodec
  ](
     config: JournalConfig,
     origin: Option[Origin],
     eventualJournal: EventualJournal[F],
     journalMetrics: Option[JournalMetrics[F]],
     conversionMetrics: Option[ConversionMetrics[F]],
     consumerPoolConfig: ConsumerPoolConfig,
     consumerPoolMetrics: Option[ConsumerPoolMetrics[F]],
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
      log       <- LogOf[F].apply(Journals.getClass).toResource
      headCache <- headCache
      consumer  <- ConsumerPool.of[F](consumerPoolConfig, consumerPoolMetrics, consumer)
    } yield {
      val withLog = apply(
        origin,
        producer,
        consumer,
        eventualJournal,
        headCache,
        log,
        conversionMetrics
      )
        .withLog(log, callTimeThresholds)
      journalMetrics.fold(withLog) { metrics => withLog.withMetrics(metrics) }
    }
  }

  def apply[F[_]: Concurrent: Clock: RandomIdOf: Fail: JsonCodec: MeasureDuration](
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

    apply[F](
      eventual = eventualJournal,
      consumeActionRecords = ConsumeActionRecords[F](consumer),
      produce = Produce[F](producer, origin),
      headCache = headCache,
      log = log,
      conversionMetrics = conversionMetrics)
  }

  def apply[F[_]: BracketThrow: RandomIdOf: MeasureDuration](
    eventual: EventualJournal[F],
    consumeActionRecords: ConsumeActionRecords[F],
    produce: Produce[F],
    headCache: HeadCache[F],
    log: Log[F],
    conversionMetrics: Option[ConversionMetrics[F]]
  ): Journals[F] = {

    val appendMarker = AppendMarker(produce)
    val appendEvents = AppendEvents(produce)

    def kafkaWriteWithMetrics[A](implicit kafkaWrite: KafkaWrite[F, A]) =
      conversionMetrics.fold(kafkaWrite) { metrics =>
        kafkaWrite.withMetrics(metrics.kafkaWrite)
    }

    def kafkaReadWithMetrics[A](implicit kafkaRead: KafkaRead[F, A]) =
      conversionMetrics.fold(kafkaRead) { metrics =>
        kafkaRead.withMetrics(metrics.kafkaRead)
      }

    def headAndStream(key: Key, from: SeqNr): F[(HeadInfo, F[StreamActionRecords[F]])] = {
      for {
        marker   <- appendMarker(key)
        result   <- {
          marker
            .offset
            .dec[Try]
            .toOption.fold {
            (HeadInfo.empty, StreamActionRecords.empty[F].pure[F]).pure[F]
          } { offset =>
            def stream = eventual
              .offset(key.topic, marker.partition)
              .map { offset =>
                StreamActionRecords(key, from, marker, offset, consumeActionRecords)
              }
            headCache
              .get(key, marker.partition, offset)
              .flatMap {
                case Some(headInfo) =>
                  (headInfo, stream).pure[F]
                case None           =>
                  for {
                    stream   <- stream
                    headInfo <- stream(none).fold(HeadInfo.empty) { (info, action) =>
                      info(action.action.header, action.offset)
                    }
                  } yield {
                    (headInfo, stream.pure[F])
                  }
              }
          }
        }
      } yield result
    }

    class Main

    new Main with Journals[F] {

      def apply(key: Key) = {
        new Main with Journal[F] {

          def append[A](events: Nel[Event[A]], metadata: RecordMetadata, headers: Headers)(
            implicit kafkaWrite: KafkaWrite[F, A]) = {
            appendEvents(key, events, metadata, headers)(kafkaWriteWithMetrics)
          }

          def read[A](from: SeqNr)(implicit kafkaRead: KafkaRead[F, A], eventualRead: EventualRead[F, A]) = {

            def readEventual(from: SeqNr) = {
              eventual
                .read(key, from)
                .mapM { _.traverse(eventualRead.apply) }
            }


            def read(head: HeadInfo, stream: F[StreamActionRecords[F]]) = {

              def empty = Stream.empty[F, EventRecord[A]]

              head match {
                case HeadInfo.Empty     =>
                  readEventual(from)

                case a: HeadInfo.Append =>

                  def read(seqNr: SeqNr) = {
                    for {
                      stream    <- stream.toStream
                      readKafka  = (seqNr: SeqNr, offset0: Option[Offset]) => {
                        // TODO refactor this, pass from offset, rather than from - 1
                        val offset = a
                          .offset
                          .dec[Try]
                          .toOption
                          .max { offset0 }
                        val records = for {
                          record <- stream(offset)
                          action <- record.action match {
                            case a: Action.Append => Stream[F].single(a)
                            case _: Action.Delete => Stream[F].empty
                            case _: Action.Purge  => Stream[F].empty
                          }
                          if action.range.to >= seqNr
                          events <- kafkaReadWithMetrics
                            .apply(action.toPayloadAndType)
                            .toStream
                          event  <- events
                            .events
                            .toStream1[F]
                          if event.seqNr >= seqNr
                        } yield {
                          EventRecord(action, event, record.partitionOffset, events.metadata)
                        }
                        records.stateful(seqNr) { case (seqNr, a) =>
                          if (seqNr <= a.seqNr) {
                            val seqNr1 = a
                              .seqNr
                              .next[Option]
                            (seqNr1, Stream[F].single(a))
                          } else {
                            (seqNr.some, Stream[F].empty[EventRecord[A]])
                          }
                        }
                      }
                      event     <- readEventual(seqNr).flatMapLast {
                        case Some(event) =>
                          event
                            .seqNr
                            .next[Option]
                            .fold {
                              Stream[F].empty[EventRecord[A]]
                            } { seqNr =>
                              readKafka(seqNr, event.offset.some)
                            }
                        case None        =>
                          readKafka(seqNr, none)
                      }
                    } yield event
                  }

                  a
                    .deleteTo
                    .fold {
                      read(from)
                    } { deleteTo =>
                      deleteTo
                        .value
                        .next[Option]
                        .fold(empty) { min => read(min max from) }
                    }

                case a: HeadInfo.Delete =>
                  a
                    .deleteTo
                    .value
                    .next[Option]
                    .fold(empty) { min => readEventual(min max from) }

                case HeadInfo.Purge     =>
                  empty
              }
            }

            for {
              headAndStream  <- headAndStream(key, from).toStream
              (head, stream)  = headAndStream
              _              <- log.debug(s"$key read info: $head").toStream
              eventRecord    <- read(head, stream)
            } yield eventRecord
          }


          def pointer = {

            // TODO refactor, we don't need to call `eventual.pointer` without using it's offset
            def pointerEventual = {
              eventual
                .pointer(key)
                .map { pointer =>
                  pointer.map { _.seqNr }
                }
            }
            for {
              headAndStream <- headAndStream(key, SeqNr.min)
              (headInfo, _)  = headAndStream
              pointer       <- headInfo match {
                case HeadInfo.Empty     => pointerEventual
                case a: HeadInfo.Append => a.seqNr.some.pure[F]
                case _: HeadInfo.Delete => pointerEventual
                case HeadInfo.Purge     => none[SeqNr].pure[F]
              }
            } yield pointer
          }

          // TODO not delete already deleted, do not accept deleteTo=2 when already deleteTo=3
          def delete(to: DeleteTo) = {
            pointer.flatMap { seqNr =>
              seqNr.traverse { seqNr =>
                produce.delete(key, seqNr.toDeleteTo min to)
              }
            }
          }

          def purge: F[Option[PartitionOffset]] = {
            produce
              .purge(key)
              .map { _.some }
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

    def apply[F[_] : Monad : Fail](
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
