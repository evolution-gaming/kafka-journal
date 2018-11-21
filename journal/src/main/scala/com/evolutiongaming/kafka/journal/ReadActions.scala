package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.IO.implicits._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer.Consumer

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

trait ReadActions[F[_]] {
  def apply(): F[Iterable[ActionRecord[Action]]]
}

object ReadActions {

  def apply[F[_] : IO : FromFuture](
    key: Key,
    consumer: Consumer[Id, Bytes, Future],
    timeout: FiniteDuration,
    log: ActorLog)(implicit ec: ExecutionContext): ReadActions[F] = {

    new ReadActions[F] {

      def apply() = {
        for {
          consumerRecords <- FromFuture[F].apply { consumer.poll(timeout) }
        } yield {
          for {
            consumerRecords <- consumerRecords.values.values
            consumerRecord <- consumerRecords
            if consumerRecord.key.exists(_.value == key.id)
            action <- consumerRecord.toAction
          } yield {
            val partitionOffset = PartitionOffset(consumerRecord)
            log.debug {
              val name = action match {
                case action: Action.Append => s"append, range: ${ action.range }"
                case action: Action.Delete => s"delete, to: ${ action.to }"
                case action: Action.Mark   => s"mark, id: ${ action.id }"
              }
              s"$key read $name, offset: $partitionOffset, origin: ${ action.origin }"
            }
            ActionRecord(action, partitionOffset)
          }
        }
      }
    }
  }
}
