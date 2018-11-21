package com.evolutiongaming.kafka.journal

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.nel.Nel

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

object FoldWhileHelper {


  implicit class NelFoldWhile[E](val self: Nel[E]) extends AnyVal {
    def foldWhile[S](s: S)(f: Fold[S, E]): Switch[S] = {
      self.toList.foldWhile(s)(f)
    }
  }


  implicit class FutureFoldWhile[S](val self: S => Future[Switch[S]]) extends AnyVal {

    def foldWhile(s: S)(implicit ec: ExecutionContext): Future[S] = {
      for {
        switch <- self(s)
        s = switch.s
        s <- if (switch.stop) s.future else foldWhile(s)
      } yield s
    }
  }


  implicit class FoldWhileIOOps[S, F[_]](val self: S => F[Switch[S]]) extends AnyVal {
    def foldWhile(s: S)(implicit io: IO[F]): F[S] = {
      io.foldWhile1(s)(self)
    }
  }


  implicit class SourceObjFoldWhile(val self: Source.type) extends AnyVal {

    def foldWhile[S, E](s: S)(f: S => Future[(Switch[S], Iterable[E])]): Source[E, NotUsed] = {
      implicit val ec = CurrentThreadExecutionContext
      val source = Source.unfoldAsync[Switch[S], Iterable[E]](s.continue) { switch =>
        if (switch.stop) Future.none
        else {
          for {
            (switch, es) <- f(switch.s)
          } yield {
            if (switch.stop || es.isEmpty) None
            else Some((switch, es))
          }
        }
      }
      source.mapConcat(_.to[immutable.Iterable])
    }
  }
}
