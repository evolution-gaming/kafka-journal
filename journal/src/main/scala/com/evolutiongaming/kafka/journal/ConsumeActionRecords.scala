package com.evolutiongaming.kafka.journal

import cats.data.NonEmptySet as Nes
import cats.effect.Resource
import cats.syntax.all.*
import cats.~>
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.kafka.journal.conversions.ConsRecordToActionRecord
import com.evolutiongaming.kafka.journal.util.StreamHelper.*
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}
import com.evolutiongaming.sstream.Stream

trait ConsumeActionRecords[F[_]] {

  def apply(key: Key, partition: Partition, from: Offset): Stream[F, ActionRecord[Action]]
}

object ConsumeActionRecords {

  def apply[F[_]: BracketThrowable](
    consumer: Resource[F, Journals.Consumer[F]],
  )(implicit consRecordToActionRecord: ConsRecordToActionRecord[F]): ConsumeActionRecords[F] = {
    class Main
    new Main with ConsumeActionRecords[F] {
      def apply(key: Key, partition: Partition, from: Offset) = {
        for {
          consumer <- consumer.toStream
          _ <- Stream.lift {
            val topicPartition = TopicPartition(topic = key.topic, partition = partition)
            for {
              _ <- consumer.assign(Nes.of(topicPartition))
              a <- consumer.seek(topicPartition, from)
            } yield a
          }
          records <- Stream.repeat {
            for {
              records <- consumer.poll
              actions <- records
                .values
                .values
                .flatMap { records =>
                  records
                    .toList
                    .filter { record =>
                      record.key.exists { _.value === key.id }
                    }
                }
                .toList
                .traverseFilter { record => consRecordToActionRecord(record) }
            } yield actions
          }
          record <- records.toStream1[F]
        } yield record
      }
    }
  }

  private sealed abstract class MapK

  implicit class ConsumeActionRecordsOps[F[_]](val self: ConsumeActionRecords[F]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G, gf: G ~> F): ConsumeActionRecords[G] = {
      new MapK with ConsumeActionRecords[G] {
        def apply(key: Key, partition: Partition, from: Offset) = {
          self(key, partition, from).mapK[G](fg, gf)
        }
      }
    }
  }
}
