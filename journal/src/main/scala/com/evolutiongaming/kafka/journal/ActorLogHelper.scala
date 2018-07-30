package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.safeakka.actor.ActorLog

import scala.compat.Platform
import scala.util.{Failure, Success}

object ActorLogHelper {

  implicit class ActorLogOps(val self: ActorLog) extends AnyVal {
    import ActorLogOps._

    // TODO support Option
    def apply[T](name: => String, toStr: ToStr[T] = ToStr.Default)(async: Async[T]): Async[T] = {
      val time = Platform.currentTime
      async.mapTry { result =>
        val duration = Platform.currentTime - time
        result match {
          case Success(())      => self.debug(s"$name in ${ duration }ms")
          case Success(result)  => self.debug(s"$name, result in ${ duration }ms: ${ toStr(result) }")
          case Failure(failure) => self.error(s"$name, failed in ${ duration }ms: $failure", failure)
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
