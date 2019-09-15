package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import cats.effect.{Resource, Sync}
import cats.implicits._
import cats.~>
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.journal.conversions.ConsumerRecordToActionRecord
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}


trait ReadActionsOf[F[_]] {

  def apply(key: Key, partition: Partition, from: Offset): Resource[F, ReadActions.Type[F]]
}

object ReadActionsOf {

  def apply[F[_] : Sync/*TODO sync?*/](
    consumer: Resource[F, Journal.Consumer[F]],
    log: Log[F])(implicit
    consumerRecordToActionRecord: ConsumerRecordToActionRecord[F]
  ): ReadActionsOf[F] = {
    (key: Key, partition: Partition, from: Offset) => {

      val topicPartition = TopicPartition(topic = key.topic, partition = partition)

      def readActions(consumer: Journal.Consumer[F]) = {
        for {
          _ <- consumer.assign(Nel.of(topicPartition))
          _ <- consumer.seek(topicPartition, from)
          _ <- log.debug(s"$key consuming from $partition:$from")
        } yield {
          ReadActions[F](key, consumer)
        }
      }

      for {
        consumer    <- consumer
        readActions <- Resource.liftF(readActions(consumer))
      } yield readActions
    }
  }


  implicit class ReadActionsOfOps[F[_]](val self: ReadActionsOf[F]) extends AnyVal {

    def mapK[G[_] : Sync](f: F ~> G)(implicit F: Sync[F]): ReadActionsOf[G] = new ReadActionsOf[G] {

      def apply(key: Key, partition: Partition, from: Offset) = {
        for {
          a <- self(key, partition, from).mapK[G](f)
        } yield {
          f(a)
        }
      }
    }
  }
}