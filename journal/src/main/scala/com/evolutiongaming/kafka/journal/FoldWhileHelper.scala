package com.evolutiongaming.kafka.journal

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.nel.Nel

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.ControlThrowable
import scala.util.{Failure, Success}

object FoldWhileHelper {


  implicit class IterableFoldWhile[E](val self: Iterable[E]) extends AnyVal {
    import IterableFoldWhile._

    def foldWhile[S](s: S)(f: Fold[S, E]): Switch[S] = {
      try {
        val ss = self.foldLeft(s) { (s, e) =>
          val switch = f(s, e)
          if (switch.stop) throw Return(switch) else switch.s
        }
        ss.continue
      } catch {
        case Return(switch) => switch.asInstanceOf[Switch[S]]
      }
    }
  }


  object IterableFoldWhile {
    private final case class Return[S](s: S) extends ControlThrowable
  }


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


  implicit class AsyncFoldWhile[S](val self: S => Async[Switch[S]]) extends AnyVal {

    def foldWhile(s: S): Async[S] = {
      import com.evolutiongaming.concurrent.async.Async._

      @tailrec def foldWhile(switch: Switch[S]): Async[S] = {
        if (switch.stop) switch.s.async
        else {
          self(switch.s) match {
            case Succeed(s)                => foldWhile(s)
            case Failed(s)                 => Failed(s)
            case s: InCompleted[Switch[S]] => s.value() match {
              case Some(Success(s)) => foldWhile(s)
              case Some(Failure(s)) => Failed(s)
              case None             => s.flatMap(break)
            }
          }
        }
      }

      def break(switch: Switch[S]): Async[S] = foldWhile(switch)

      foldWhile(s.continue)
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
