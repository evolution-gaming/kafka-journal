package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem, NonEmptySet => Nes}
import cats.effect._
import cats.effect.implicits._
import cats.implicits._
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf, MeasureDuration, ToTry}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.conversions.{ConsRecordToActionRecord, KafkaRead}
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.catshelper.DataHelper._
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.retry.Sleep
import com.evolutiongaming.skafka.{Metadata, Offset, Partition, Topic}
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import scodec.bits.ByteVector

import java.time.Instant
import scala.concurrent.duration._
import scala.util.Try

object TopicReplicator {

  def of[F[_]: Concurrent: Sleep: ToTry: LogOf: Fail: MeasureDuration: JsonCodec](
    topic: Topic,
    journal: ReplicatedJournal[F],
    consumer: Resource[F, TopicConsumer[F]],
    metrics: TopicReplicatorMetrics[F],
    cacheOf: CacheOf[F]
  ): Resource[F, F[Outcome[F, Throwable, Unit]]] = {

    implicit val fromAttempt: FromAttempt[F] = FromAttempt.lift[F]
    implicit val fromJsResult: FromJsResult[F] = FromJsResult.lift[F]
    implicit val jsonCodec: JsonCodec[Try] = JsonCodec.summon[F].mapK(ToTry.functionK)

    val kafkaRead = KafkaRead.summon[F, Payload]
    val eventualWrite = EventualWrite.summon[F, Payload]

    def consume(
      consumer: Resource[F, TopicConsumer[F]],
      log: Log[F]
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
        cacheOf = cacheOf)
    }

    def log = for {
      log <- LogOf[F].apply(TopicReplicator.getClass)
    } yield {
      log prefixed topic
    }

    for {
      log <- log.toResource
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
    cacheOf: CacheOf[F]
  ): F[Unit] = {

    trait PartitionFlow {
      def apply(timestamp: Instant, records: Nel[ConsRecord]): F[Unit]
    }

    trait KeyFlow {
      def apply(timestamp: Instant, records: Nel[ConsRecord]): F[Unit]
    }

    val topicFlowOf: TopicFlowOf[F] = {
      (topic: Topic) => {
        for {
          journal <- journal.journal(topic)
          cache <- cacheOf[Partition, PartitionFlow](topic)
        } yield {

          def keyFlow(id: String): Resource[F, KeyFlow] = {
            for {
              journal <- journal.journal(id)
            } yield {
              val replicateRecords = ReplicateRecords(
                consRecordToActionRecord = consRecordToActionRecord,
                journal = journal,
                metrics = metrics,
                kafkaRead,
                eventualWrite,
                log = log)

              (timestamp: Instant, records: Nel[ConsRecord]) => {
                replicateRecords(records, timestamp)
              }
            }
          }


          def partitionFlow: Resource[F, PartitionFlow] = {
            for {
              cache <- cacheOf[String, KeyFlow](topic)
            } yield {
              (timestamp: Instant, records: Nel[ConsRecord]) => {
                records
                  .groupBy { _.key.map { _.value } }
                  .parFoldMap1 { case (key, records) =>
                    key.foldMapM { key =>
                      for {
                        keyFlow <- cache.getOrUpdate(key) { keyFlow(key) }
                        result <- keyFlow(timestamp, records)
                      } yield result
                    }
                  }
              }
            }
          }

          def remove(partitions: Nes[Partition]) = {
            partitions.parFoldMap1 { partition => cache.remove(partition) }
          }

          new TopicFlow[F] {

            def assign(partitions: Nes[Partition]) = {
              log.info(s"assign ${ partitions.mkString_(",") }")
            }

            def apply(records: Nem[Partition, Nel[ConsRecord]]) = {

              def records1 =
                records.toSortedMap.toList.flatTraverse {
                  case (partition, records) =>
                    journal.pointer(partition).map { offset =>
                      for {
                        records <- offset.fold(records.some) { offset => records.filter { _.offset > offset }.toNel }.toList
                      } yield {
                        (partition, records)
                      }
                    }
                }.map(_.toSortedMap)

              def replicateTopic(timestamp: Instant, records: Nem[Partition, Nel[ConsRecord]]) = {

                def pointers = records.map { records =>
                  records
                    .maximumBy { _.offset }
                    .offset
                }

                def replicate = {
                  records.parFoldMap1 { case (partition, records) =>
                    for {
                      partitionFlow <- cache.getOrUpdate(partition) { partitionFlow }
                      result <- partitionFlow(timestamp, records)
                    } yield result
                  }
                }

                for {
                  _ <- replicate
                  _ <- journal.save(pointers, timestamp)
                } yield {}
              }

              for {
                duration <- MeasureDuration[F].start
                timestamp <- Clock[F].instant
                records1 <- records1
                _ <- records1.toNem().foldMapM { records => replicateTopic(timestamp, records) }
                size = records.foldLeft(0) { case (size, records) => size + records.size }
                duration <- duration
                _ <- metrics.round(duration, size)
              } yield {
                records
                  .map { records =>
                    records.foldLeft { Offset.min } { (offset, record) =>
                      record
                        .offset
                        .inc[Try]
                        .fold(_ => offset, _ max offset)
                    }
                  }
                  .toSortedMap
              }
            }

            def revoke(partitions: Nes[Partition]) = {
              for {
                _ <- log.info(s"revoke ${ partitions.mkString_(",") }")
                a <- remove(partitions)
              } yield a
            }

            def lose(partitions: Nes[Partition]) = {
              for {
                _ <- log.info(s"lose ${ partitions.mkString_(",") }")
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

    def of[F[_]: Concurrent: KafkaConsumerOf: FromTry: Clock](
      topic: Topic,
      config: ConsumerConfig,
      pollTimeout: FiniteDuration,
      hostName: Option[HostName]
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
        autoCommit = false)

      for {
        consumer <- KafkaConsumerOf[F].apply[String, ByteVector](config1)
        metadata = hostName.fold { Metadata.empty } { _.value }
        commit = TopicCommit(topic, metadata, consumer)
        commit <- TopicCommit.delayed(5.seconds, commit).toResource
      } yield {
        TopicConsumer(topic, pollTimeout, commit, consumer)
      }
    }
  }
}
