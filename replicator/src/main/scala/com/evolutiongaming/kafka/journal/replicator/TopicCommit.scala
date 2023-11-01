package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant
import cats.Applicative
import cats.data.{NonEmptyMap => Nem}
import cats.effect.kernel.Concurrent
import cats.effect.{Clock, Ref, Sync}
import cats.syntax.all._
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.DataHelper._
import com.evolutiongaming.kafka.journal.util.TemporalHelper._
import com.evolutiongaming.kafka.journal.{KafkaConsumer, KafkaProducer}
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.producer.ProducerRecord
import com.evolutiongaming.skafka.ToBytes

import java.nio.ByteBuffer
import scala.collection.immutable.SortedMap
import scala.concurrent.duration._

trait TopicCommit[F[_]] {

  def apply(offsets: Nem[Partition, Offset]): F[Unit]
}

object TopicCommit {

  def empty[F[_] : Applicative]: TopicCommit[F] = (_: Nem[Partition, Offset]) => ().pure[F]

  def apply[F[_]](
    topic: Topic,
    metadata: String,
    consumer: KafkaConsumer[F, _, _],
  ): TopicCommit[F] = {
    offsets: Nem[Partition, Offset] => {
      val offsets1 = offsets.mapKV { (partition, offset) =>
        val offset1 = OffsetAndMetadata(offset, metadata)
        val partition1 = TopicPartition(topic, partition)
        (partition1, offset1)
      }
      consumer.commit(offsets1)
    }
  }

  def delayed[F[_] : Concurrent : Clock](
    delay: FiniteDuration,
    commit: TopicCommit[F]
  ): F[TopicCommit[F]] = {

    case class State(until: Instant, offsets: SortedMap[Partition, Offset] = SortedMap.empty)

    for {
      timestamp <- Clock[F].instant
      stateRef  <- Ref[F].of(State(timestamp + delay))
    } yield {
      new TopicCommit[F] {
        def apply(offsets: Nem[Partition, Offset]) = {

          def apply(state: State, timestamp: Instant) = {
            val offsets1 = state.offsets ++ offsets.toSortedMap
            if (state.until <= timestamp) {
              offsets1
                .toNem()
                .foldMapM { offsets => commit(offsets) }
                .as(State(timestamp + delay))
            } else {
              state
                .copy(offsets = offsets1)
                .pure[F]
            }
          }

          for {
            timestamp <- Clock[F].instant
            state     <- stateRef.get
            state     <- apply(state, timestamp)
            _         <- stateRef.set(state)
          } yield {}
        }
      }
    }
  }

  def pointer[F[_]: Sync](
    topic: Topic,
    producer: KafkaProducer[F],
    commit: TopicCommit[F]
  ): TopicCommit[F] =
    new TopicCommit[F] {

      implicit val partitionToBytes: ToBytes[F, Partition] =
        (partition: Partition, _) => Sync[F].delay { ByteBuffer.allocate(4).putInt(partition.value).array() }

      implicit val offsetToBytes: ToBytes[F, Offset] =
        (offset: Offset, _) => Sync[F].delay { ByteBuffer.allocate(8).putLong(offset.value).array() }

      override def apply(offsets: Nem[Partition, Offset]): F[Unit] = {

        val commitPointers = offsets.toNel.toList.traverse {
          case (partition, offset) =>
            val record = new ProducerRecord[Partition, Offset](
              topic = topic,
              partition = partition.some, // manually setting partition for imitating journal topic
              key = partition.some,
              value = offset.some,
            )
            producer.send(record)
        }

        val commitOffsets = commit(offsets)

        for {
          _ <- commitPointers
          _ <- commitOffsets
        } yield {}
      }
    }
}
