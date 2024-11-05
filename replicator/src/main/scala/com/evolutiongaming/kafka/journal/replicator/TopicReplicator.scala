package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList as Nel, NonEmptyMap as Nem, NonEmptySet as Nes}
import cats.effect.*
import cats.effect.implicits.*
import cats.implicits.*
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.catshelper.ParallelHelper.*
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf, MeasureDuration, ToTry}
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.kafka.journal.conversions.{ConsRecordToActionRecord, KafkaRead}
import com.evolutiongaming.kafka.journal.eventual.*
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.SkafkaHelper.*
import com.evolutiongaming.retry.Sleep
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import com.evolutiongaming.skafka.{Metadata, Offset, Partition, Topic}
import scodec.bits.ByteVector

import java.time.Instant
import scala.concurrent.duration.*
import scala.util.Try

/**
 * Consumes the Kafka topic and "splits" the data stream into [[PartitionFlow]]s
 * and "splits" each per-partition stream in [[KeyFlow]]s.
 * Basically:
 *  - result of each Kafka's `poll` gets grouped per partition and key
 *  - grouped per-key records are processed by [[ReplicateRecords]]
 */
private[journal] object TopicReplicator {

  def make[F[_]: Concurrent: Sleep: ToTry: LogOf: Fail: MeasureDuration: JsonCodec](
    topic: Topic,
    journal: ReplicatedJournal[F],
    consumer: Resource[F, TopicConsumer[F]],
    metrics: TopicReplicatorMetrics[F],
    cacheOf: CacheOf[F],
  ): Resource[F, F[Outcome[F, Throwable, Unit]]] = {

    implicit val fromAttempt: FromAttempt[F]   = FromAttempt.lift[F]
    implicit val fromJsResult: FromJsResult[F] = FromJsResult.lift[F]
    implicit val jsonCodec: JsonCodec[Try]     = JsonCodec.summon[F].mapK(ToTry.functionK)

    val kafkaRead     = KafkaRead.summon[F, Payload]
    val eventualWrite = EventualWrite.summon[F, Payload]

    def consume(
      consumer: Resource[F, TopicConsumer[F]],
      log: Log[F],
    ) = {

      val consRecordToActionRecord = ConsRecordToActionRecord[F]
      of(
        topic                    = topic,
        consumer                 = consumer,
        consRecordToActionRecord = consRecordToActionRecord,
        kafkaRead                = kafkaRead,
        eventualWrite            = eventualWrite,
        journal                  = journal,
        metrics                  = metrics,
        log                      = log,
        cacheOf                  = cacheOf,
      )
    }

    def log = for {
      log <- LogOf[F].apply(TopicReplicator.getClass)
    } yield {
      log prefixed topic
    }

    for {
      log  <- log.toResource
      done <- consume(consumer, log).background
    } yield done
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
  ): F[Unit] = {

    trait PartitionFlow {
      def apply(timestamp: Instant, records: Nel[ConsRecord]): F[Unit]
    }

    trait KeyFlow {
      def apply(timestamp: Instant, records: Nel[ConsRecord]): F[Int]
    }

    val topicFlowOf: TopicFlowOf[F] = { (topic: Topic) =>
      {
        for {
          journal <- journal.journal(topic)
          cache   <- cacheOf[Partition, PartitionFlow](topic)
        } yield {

          def remove(partitions: Nes[Partition]) = {
            partitions.parFoldMap1 { partition => cache.remove(partition) }
          }

          new TopicFlow[F] {

            def assign(partitions: Nes[Partition]) = {
              log.info(s"assign ${partitions.mkString_(",")}")
            }

            def apply(records: Nem[Partition, Nel[ConsRecord]]) = {
              for {
                duration  <- MeasureDuration[F].start
                timestamp <- Clock[F].instant
                _ <- records.parFoldMap1 {
                  case (partition, records) =>
                    for {
                      partitionFlow <- cache.getOrUpdate(partition) {
                        for {
                          journal <- journal(partition)
                          offsets  = journal.offsets
                          offsetRef <- Resource.eval {
                            for {
                              offset <- offsets.get
                              ref    <- Ref.of(offset)
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
                              result <- {
                                val offset1 = records.maximumBy { _.offset }.offset

                                def set = offsetRef.set(offset1.some)

                                offset.fold {
                                  for {
                                    a <- offsets.create(offset1, timestamp)
                                    _ <- set
                                  } yield a
                                } { offset =>
                                  if (offset1 > offset) {
                                    for {
                                      a <- offsets.update(offset1, timestamp)
                                      _ <- set
                                    } yield a
                                  } else {
                                    ().pure[F]
                                  }
                                }
                              }
                            } yield result
                          }
                        }
                      }
                      result <- partitionFlow(timestamp, records)
                    } yield result
                }
                duration <- duration
                _        <- metrics.round(duration, records.foldLeft(0) { _ + _.size })
              } yield {
                records.map { records =>
                  records.foldLeft { Offset.min } { (offset, record) =>
                    record
                      .offset
                      .inc[Try]
                      .fold(_ => offset, _ max offset)
                  }
                }.toSortedMap
              }
            }

            def revoke(partitions: Nes[Partition]) = {
              for {
                _ <- log.info(s"revoke ${partitions.mkString_(",")}")
                a <- remove(partitions)
              } yield a
            }

            def lose(partitions: Nes[Partition]) = {
              for {
                _ <- log.info(s"lose ${partitions.mkString_(",")}")
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

    def make[F[_]: Concurrent: KafkaConsumerOf: FromTry: Clock](
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
        common          = common.copy(clientId = clientId.some),
        groupId         = groupId.some,
        autoOffsetReset = AutoOffsetReset.Earliest,
        autoCommit      = false,
      )

      for {
        consumer <- KafkaConsumerOf[F].apply[String, ByteVector](config1)
        metadata  = hostName.fold { Metadata.empty } { _.value }
        commit    = TopicCommit(topic, metadata, consumer)
        commit   <- TopicCommit.delayed(5.seconds, commit).toResource
      } yield {
        TopicConsumer(topic, pollTimeout, commit, consumer)
      }
    }
  }
}
