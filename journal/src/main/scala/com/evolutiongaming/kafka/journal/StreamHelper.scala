package com.evolutiongaming.kafka.journal

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.kafka.journal.FutureHelper._

import scala.collection.immutable.Iterable
import scala.concurrent.Future

object StreamHelper {
  implicit class SourceObjOps(val self: Source.type) extends AnyVal {

    // TODO adapt to FastFuture ?
    // TODO return Future[S]
    def foldWhile[S, E](s: S)(f: S => Future[(S, Boolean, Iterable[E])]): Source[E, NotUsed] = {
      implicit val ec = CurrentThreadExecutionContext
      val zero = (s, true)
      val source = Source.unfoldAsync[(S, Boolean), Iterable[E]](zero) {
        case (s, false) => Future.none
        case (s, true)  =>
          for {
            (s, b, es) <- f(s)
          } yield {
            if (es.isEmpty && !b) None
            else Some(((s, b), es))
          }
      }
      source.mapConcat(identity)
    }
  }
}
