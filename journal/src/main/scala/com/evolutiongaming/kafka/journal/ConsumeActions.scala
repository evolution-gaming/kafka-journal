package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Alias.{Bytes, Id}
import com.evolutiongaming.skafka.consumer.ConsumerRecord

import scala.concurrent.Future

trait ConsumeActions {
  def apply[S](s: S)(f: (Action, ConsumerRecord[Id, Bytes]) => Option[S]): Future[S]
}

object ConsumeActions {

  def apply(): ConsumeActions = {

    new ConsumeActions {

      def apply[S](s: S)(f: (Action, ConsumerRecord[Id, Bytes]) => Option[S]): Future[S] = {
        ???
      }
    }
  }

}
