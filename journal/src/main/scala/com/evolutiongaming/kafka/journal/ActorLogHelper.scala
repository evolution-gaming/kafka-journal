package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.safeakka.actor.ActorLog

import scala.util.{Failure, Success}

object ActorLogHelper {

  implicit class ActorLogOps(val self: ActorLog) extends AnyVal {
    import ActorLogOps._

    def apply[T](name: => String, toStr: ToStr[T] = ToStr.Default)(async: Async[T]): Async[T] = {
      async.mapTry { result =>
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
