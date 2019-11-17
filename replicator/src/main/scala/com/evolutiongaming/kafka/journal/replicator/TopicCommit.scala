package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import cats.data.{NonEmptyMap => Nem}
import cats.effect.concurrent.Ref
import cats.effect.{Clock, Sync}
import cats.implicits._
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.kafka.journal.util.CollectionHelper._
import com.evolutiongaming.kafka.journal.util.TemporalHelper._
import com.evolutiongaming.kafka.journal.KafkaConsumer
import com.evolutiongaming.skafka._

import scala.collection.immutable.SortedMap
import scala.concurrent.duration._

trait TopicCommit[F[_]] {

  def apply(offsets: Nem[Partition, Offset]): F[Unit]
}

object TopicCommit {

  def apply[F[_]](
    topic: Topic,
    metadata: String,
    consumer: KafkaConsumer[F, _, _],
  ): TopicCommit[F] = {
    (offsets: Nem[Partition, Offset]) => {
      val offsets1 = offsets.mapKV { (partition, offset) =>
        val offset1 = OffsetAndMetadata(offset, metadata)
        val partition1 = TopicPartition(topic, partition)
        (partition1, offset1)
      }
      consumer.commit(offsets1)
    }
  }

  def delayed[F[_] : Sync : Clock](
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
                .toNem
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
}