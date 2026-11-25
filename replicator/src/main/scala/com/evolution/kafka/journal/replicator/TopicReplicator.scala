package com.evolution.kafka.journal.replicator

import cats.Monad
import cats.data.{NonEmptyList as Nel, NonEmptyMap as Nem, NonEmptySet as Nes}
import cats.effect.*
import cats.effect.implicits.*
import cats.syntax.all.*
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.conversions.{ConsRecordToActionRecord, KafkaRead}
import com.evolution.kafka.journal.eventual.*
import com.evolution.kafka.journal.util.Fail
import com.evolution.kafka.journal.util.SkafkaHelper.*
import com.evolutiongaming.catshelper.*
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.catshelper.ParallelHelper.*
import com.evolutiongaming.retry.Sleep
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import scodec.bits.ByteVector

import java.time.Instant
import scala.concurrent.duration.*
import scala.util.Try

/**
 * Consumes the Kafka topic and "splits" the data stream into [[PartitionFlow]]s and "splits" each
 * per-partition stream in [[KeyFlow]]s.
 *
 * Basically:
 *   - result of each Kafka's `poll` gets grouped per partition and key
 *   - grouped per-key records are processed by [[ReplicateRecords]]
 */
private[journal] object TopicReplicator {

  private val pointerWritePeriod: FiniteDuration = 50.millis

  def make[F[_]: Concurrent: Sleep: ToTry: LogOf: Fail: MeasureDuration: JsonCodec](
    topic: Topic,
    journal: ReplicatedJournal[F],
    consumer: Resource[F, TopicConsumer[F]],
    metrics: TopicReplicatorMetrics[F],
    cacheOf: CacheOf[F],
    replicatedOffsetNotifier: ReplicatedOffsetNotifier[F],
  ): Resource[F, F[Outcome[F, Throwable, Unit]]] = {

    implicit val fromAttempt: FromAttempt[F] = FromAttempt.lift[F]
    implicit val fromJsResult: FromJsResult[F] = FromJsResult.lift[F]
    implicit val jsonCodec: JsonCodec[Try] = JsonCodec.summon[F].mapK(ToTry.functionK)

    val kafkaRead = KafkaRead.summon[F, Payload]
    val eventualWrite = EventualWrite.summon[F, Payload]

    def consume(
      consumer: Resource[F, TopicConsumer[F]],
      log: Log[F],
    ) = {

      val consRecordToActionRecord = ConsRecordToActionRecord[F]
      of(
        topic = topic,
        consumer = consumer,
        consRecordToActionRecord = consRecordToActionRecord,
        kafkaRead = kafkaRead,
        eventualWrite = eventualWrite,
        journal = journal,
        metrics = metrics,
        log = log,
        cacheOf = cacheOf,
        replicatedOffsetNotifier = replicatedOffsetNotifier,
        pointerWriterOf = PointerWriter.Of.periodic[F](pointerWritePeriod, log),
      )
    }

    for {
      log <- topicLoggerOf(topic).toResource
      done <- consume(consumer, log).background
    } yield done
  }

  private def topicLoggerOf[F[_]: LogOf: Monad](topic: Topic): F[Log[F]] = {
    LogOf[F].apply(TopicReplicator.getClass).map(_ prefixed topic)
  }

  def of[F[_]: Concurrent: Sleep: MeasureDuration, A](
    topic: Topic,
    consumer: Resource[F, TopicConsumer[F]],
    consRecordToActionRecord: ConsRecordToActionRecord[F],
    kafkaRead: KafkaRead[F, A],
    eventualWrite: EventualWrite[F, A],
    journal: ReplicatedJournal[F],
    metrics: TopicReplicatorMetrics[F],
    log: Log[F],
    cacheOf: CacheOf[F],
    replicatedOffsetNotifier: ReplicatedOffsetNotifier[F],
    pointerWriterOf: PointerWriter.Of[F],
  ): F[Unit] = {

    trait PartitionFlow {
      def apply(timestamp: Instant, records: Nel[ConsRecord]): F[Unit]
    }

    trait KeyFlow {
      def apply(timestamp: Instant, records: Nel[ConsRecord]): F[Int]
    }

    val topicFlowOf: TopicFlowOf[F] = { (topic: Topic, commit: TopicCommit[F]) =>
      {
        for {
          journal <- journal.journal(topic)
          pointerWriter <- pointerWriterOf.createWriter(topic, commit, replicatedOffsetNotifier)
          cache <- cacheOf[Partition, PartitionFlow](topic)
        } yield {

          def remove(partitions: Nes[Partition]) = {
            partitions.parFoldMap1 { partition => cache.remove(partition) }
          }

          new TopicFlow[F] {

            def assign(partitions: Nes[Partition]): F[Unit] = {
              log.info(s"assign ${ partitions.mkString_(",") }")
            }

            def apply(records: Nem[Partition, Nel[ConsRecord]]): F[Unit] = {
              for {
                duration <- MeasureDuration[F].start
                timestamp <- Clock[F].instant
                _ <- records.parFoldMap1 {
                  case (partition, records) =>
                    for {
                      partitionFlow <- cache.getOrUpdate(partition) {
                        for {
                          journal <- journal(partition)
                          offsets = journal.offsets
                          offsetRef <- Resource.eval {
                            for {
                              stored <- offsets.get
                              replicated <- pointerWriter.assign(partition, offsets, stored)
                              ref <- Ref.of(replicated)
                            } yield ref
                          }
                          cache <- cacheOf[String, KeyFlow](topic)
                        } yield { (timestamp: Instant, records: Nel[ConsRecord]) =>
                          {
                            for {
                              offset <- offsetRef.get
                              _ <- offset
                                .fold {
                                  records.some
                                } { offset =>
                                  records.filter { _.offset > offset }.toNel
                                }
                                .foldMapM { records =>
                                  records
                                    .groupBy { _.key.map { _.value } }
                                    .parFoldMap1 {
                                      case (key, records) =>
                                        key.foldMapM { key =>
                                          for {
                                            keyFlow <- cache.getOrUpdate(key) {
                                              journal
                                                .journal(key)
                                                .map { journal =>
                                                  val replicateRecords = ReplicateRecords(
                                                    consRecordToActionRecord,
                                                    journal,
                                                    metrics,
                                                    kafkaRead,
                                                    eventualWrite,
                                                    log,
                                                  )
                                                  (timestamp: Instant, records: Nel[ConsRecord]) => {
                                                    replicateRecords(records, timestamp)
                                                  }
                                                }
                                            }
                                            result <- keyFlow(timestamp, records)
                                          } yield result
                                        }
                                    }
                                }
                              _ <- {
                                // the pointer itself is written by `pointerWriter` after the round
                                val offset1 = records.maximumBy { _.offset }.offset
                                offsetRef.update { _.fold(offset1) { _ max offset1 }.some }
                              }
                            } yield ()
                          }
                        }
                      }
                      result <- partitionFlow(timestamp, records)
                    } yield result
                }
                _ <- pointerWriter.mark(records.map { _.maximumBy { _.offset }.offset }, timestamp)
                duration <- duration
                _ <- metrics.round(duration, records.foldLeft(0) { _ + _.size })
              } yield ()
            }

            def revoke(partitions: Nes[Partition]): F[Unit] = {
              for {
                _ <- log.info(s"revoke ${ partitions.mkString_(",") }")
                _ <- pointerWriter.revoke(partitions)
                a <- remove(partitions)
              } yield a
            }

            def lose(partitions: Nes[Partition]): F[Unit] = {
              for {
                _ <- log.info(s"lose ${ partitions.mkString_(",") }")
                _ <- pointerWriter.lose(partitions)
                a <- remove(partitions)
              } yield a
            }
          }
        }
      }
    }

    ConsumeTopic(topic, consumer, topicFlowOf, log)
  }

  object ConsumerOf {

    def make[F[_]: Temporal: KafkaConsumerOf: FromTry: LogOf](
      topic: Topic,
      config: ConsumerConfig,
      pollTimeout: FiniteDuration,
      hostName: Option[HostName],
    ): Resource[F, TopicConsumer[F]] = {

      val groupId = {
        val prefix = config.groupId getOrElse "replicator"
        s"$prefix-$topic"
      }

      val common = config.common

      val clientId = {
        val clientId = common.clientId getOrElse "replicator"
        hostName.fold(clientId) { hostName => s"$clientId-$hostName" }
      }

      val config1 = config.copy(
        common = common.copy(clientId = clientId.some),
        groupId = groupId.some,
        autoOffsetReset = AutoOffsetReset.Earliest,
        autoCommit = false,
      )

      for {
        consumer <- KafkaConsumerOf[F].apply[String, ByteVector](config1)
        log <- topicLoggerOf[F](topic).toResource
        metadata = hostName.fold { Metadata.empty } { _.value }
        commit <- TopicCommit.asyncPeriodic(
          topic = topic,
          commitMetadata = metadata,
          commitPeriod = 5.seconds,
          consumer = consumer,
          log = log,
        )
      } yield {
        TopicConsumer(topic, pollTimeout, commit, consumer)
      }
    }
  }
}
