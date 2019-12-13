package com.evolutiongaming.kafka.journal

import cats.FlatMap
import cats.effect.Clock
import cats.implicits._
import com.evolutiongaming.catshelper.ClockHelper._

trait AppendMarker[F[_]] {
  
  def apply(key: Key): F[Marker]
}

object AppendMarker {

  def apply[F[_] : FlatMap : RandomIdOf : Clock](
    appendAction: AppendAction[F],
    origin: Option[Origin]
  ): AppendMarker[F] = {

    key: Key => {
      for {
        randomId        <- RandomIdOf[F].apply
        timestamp       <- Clock[F].instant
        id               = randomId.value
        action           = Action.Mark(key, timestamp, id, origin)
        partitionOffset <- appendAction(action)
      } yield {
        Marker(id, partitionOffset)
      }
    }
  }
}
