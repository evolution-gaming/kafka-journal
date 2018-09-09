package com.evolutiongaming.kafka.journal.replicator

import akka.actor.ActorSystem
import com.evolutiongaming.concurrent.FutureHelper._

import scala.concurrent.Future
import scala.concurrent.duration._

object Retry {

  def apply[T](attempts: Int = 100, delay: FiniteDuration = 100.millis)
    (f: => Future[Option[T]])
    (implicit system: ActorSystem): Future[T] = {

    implicit val ec = system.dispatcher

    def loop(attempt: Int): Future[T] = {
      if (attempt <= 0) Future.failed(new RuntimeException(s"max attempts $attempts"))
      else for {
        result <- f
        result <- result match {
          case Some(result) => result.future
          case None         => for {
            _ <- akka.pattern.after(delay, system.scheduler)(Future.unit)
            result <- loop(attempt - 1)
          } yield result
        }
      } yield result
    }

    loop(attempts)
  }
}
