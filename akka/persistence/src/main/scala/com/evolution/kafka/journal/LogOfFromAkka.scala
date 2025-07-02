package com.evolution.kafka.journal

import akka.actor.ActorSystem
import akka.event.LogSource
import cats.effect.Sync
import cats.syntax.all.*
import com.evolutiongaming.catshelper.LogOf

object LogOfFromAkka {
  // Scala 3 compiler couldn't construct this one implicitly
  private implicit val logSourceFromClass: LogSource[Class[?]] = LogSource.fromClass

  def apply[F[_]: Sync](system: ActorSystem): LogOf[F] = {

    def log[A: LogSource](source: A) = {
      for {
        log <- Sync[F].delay { akka.event.Logging(system, source) }
      } yield {
        LogFromAkka[F](log)
      }
    }

    new LogOf[F] {

      def apply(source: String) = log(source)

      def apply(source: Class[?]) = log(source)
    }
  }
}
