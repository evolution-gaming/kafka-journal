package com.evolution.kafka.journal

import _root_.akka.actor.ActorSystem
import _root_.akka.event.LogSource
import cats.effect.Sync
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{Log, LogOf}

object LogOfFromAkka {
  // Scala 3 compiler couldn't construct this one implicitly
  private implicit val logSourceFromClass: LogSource[Class[?]] = LogSource.fromClass

  def apply[F[_]: Sync](system: ActorSystem): LogOf[F] = {

    def log[A: LogSource](source: A) = {
      for {
        log <- Sync[F].delay { _root_.akka.event.Logging(system, source) }
      } yield {
        LogFromAkka[F](log)
      }
    }

    new LogOf[F] {

      def apply(source: String): F[Log[F]] = log(source)

      def apply(source: Class[?]): F[Log[F]] = log(source)
    }
  }
}
