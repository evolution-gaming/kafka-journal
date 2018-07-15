package com.evolutiongaming.kafka.journal

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.kafka.journal.FutureHelper._

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.ControlThrowable

object FoldWhileHelper {

  type Continue = Boolean

  type FoldWhile[S, E] = (S, E) => (S, Continue)


  implicit class IterableFoldWhile[E](val self: Iterable[E]) extends AnyVal {
    import IterableFoldWhile._

    def foldWhile[S](s: S)(f: (S, E) => (S, Continue)): (S, Continue) = {
      try {
        val ss = self.foldLeft(s) { (s, e) =>
          val (ss, continue) = f(s, e)
          if (continue) ss else throw Return(ss)
        }
        (ss, true)
      } catch {
        case Return(s) => (s.asInstanceOf[S], false)
      }
    }
  }

  object IterableFoldWhile {
    private case class Return[S](s: S) extends ControlThrowable
  }


  implicit class FutureFoldWhile[S](val self: S => Future[(S, Continue)]) extends AnyVal {

    def foldWhile(s: S)(implicit ec: ExecutionContext /*TODO remove*/): Future[S] = {
      for {
        (s, continue) <- self(s)
        s <- if (continue) foldWhile(s) else s.future
      } yield s
    }
  }


  implicit class SourceObjFoldWhile(val self: Source.type) extends AnyVal {

    // TODO adapt to FastFuture ?
    // TODO return Future[S]
    def foldWhile[S, E](s: S)(f: S => Future[(S, Continue, Iterable[E])]): Source[E, NotUsed] = {
      implicit val ec = CurrentThreadExecutionContext
      val zero = (s, true)
      val source = Source.unfoldAsync[(S, Continue), Iterable[E]](zero) {
        case (_, false) => Future.none
        case (s, true)  =>
          for {
            (s, continue, es) <- f(s)
          } yield {
            if (continue || es.nonEmpty) Some(((s, continue), es))
            else None
          }
      }
      source.mapConcat(_.to[immutable.Iterable])
    }
  }
}
