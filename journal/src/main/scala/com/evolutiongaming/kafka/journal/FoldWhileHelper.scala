package com.evolutiongaming.kafka.journal

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.Monad
import cats.implicits._
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal.stream.Stream
import com.evolutiongaming.nel.Nel

import scala.collection.immutable
import scala.concurrent.Future

// TODO remove
object FoldWhileHelper {

  implicit class NelFoldWhile[A](val self: Nel[A]) extends AnyVal {

    def foldWhile[S](s: S)(f: Fold[S, A]): Switch[S] = {
      self.toList.foldWhile(s)(f)
    }

    def foldWhileM[F[_], B, S](s: S)(f: (S, A) => F[Either[S, B]])(implicit F: Monad[F]): F[Either[S, B]] = {
      self.toList.foldWhileM(s)(f)
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

  implicit class SwitchStreamOps[F[_], A](val self: Stream[F, A]) extends AnyVal {

    final def foldWhileSwitch[S](s: S)(f: (S, A) => Switch[S])(implicit F: Monad[F]): F[Switch[S]] = {
      for {
        result <- self.foldWhile(s) { (s, a) =>
          val switch = f(s, a)
          if (switch.continue) switch.s.asLeft[S] else switch.s.asRight[S]
        }
      } yield {
        result.fold(l => Switch(l, continue = true), r => Switch(r, continue = false))
      }
    }
  }
}
