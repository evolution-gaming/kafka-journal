package com.evolutiongaming.kafka.journal.replicator

import cats.effect.Sync
import cats.implicits._
import com.evolutiongaming.catshelper.{ApplicativeThrowable, MonadThrowable}

import java.time.temporal.ChronoUnit
import java.time.{LocalDateTime, LocalTime}
import scala.concurrent.duration._


trait DurationTo[F[_]] {

  def apply(localTime: LocalTime): F[FiniteDuration]
}

object DurationTo {

  def apply[F[_]: Sync]: DurationTo[F] = {
    val now = Sync[F].delay { LocalDateTime.now }
    apply(now)
  }

  def apply[F[_]: MonadThrowable](now: F[LocalDateTime]): DurationTo[F] = {
    localTime => {
      now.flatMap { now =>
        ApplicativeThrowable[F].catchNonFatal {
          val localDateTime = {
            val localDate = now.toLocalDate
            val localDateTime = localDate.atTime(localTime)
            if (localDateTime.isAfter(now)) {
              localDateTime
            } else {
              localDate
                .plusDays(1)
                .atTime(localTime)
            }
          }
          now
            .until(localDateTime, ChronoUnit.MILLIS)
            .millis
        }
      }
    }
  }
}
