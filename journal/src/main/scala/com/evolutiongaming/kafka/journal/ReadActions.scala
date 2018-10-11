package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer.Consumer

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

trait ReadActions[F[_]] {
  def apply(): F[Iterable[ActionRecord[Action]]]
}

object ReadActions {

  def apply(
    key: Key,
    consumer: Consumer[Id, Bytes],
    timeout: FiniteDuration,
    log: ActorLog)(implicit ec: ExecutionContext): ReadActions[Async] = {

    new ReadActions[Async] {

      def apply() = {
        for {
          consumerRecords <- consumer.poll(timeout).async
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
