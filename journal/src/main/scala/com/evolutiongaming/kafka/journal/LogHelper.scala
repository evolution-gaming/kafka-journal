package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.safeakka.actor.ActorLog

import scala.concurrent.Future
import scala.util.{Failure, Success}

object LogHelper {

  implicit class ActorLogOps(val self: ActorLog) extends AnyVal {
    import ActorLogOps._

    def apply[T](name: => String, toStr: ToStr[T] = ToStr.Default)(future: Future[T]): Future[T] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove
      future.transform { result =>
        result match {
          case Success(())      => self.debug(name)
          case Success(result)  => self.debug(s"$name, result: ${ toStr(result) }")
          case Failure(failure) => self.error(s"$name failed: $failure", failure)
        }
        result
      }
    }
  }

  object ActorLogOps {
    type ToStr[T] = T => String

    object ToStr {
      val Default: ToStr[Any] = (any: Any) => any.toString
    }
  }
}
