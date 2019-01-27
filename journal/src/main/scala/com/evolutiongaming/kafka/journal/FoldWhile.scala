package com.evolutiongaming.kafka.journal

import cats.Monad

import scala.util.control.ControlThrowable
import cats.implicits._
import com.evolutiongaming.kafka.journal.stream.Stream


object FoldWhile {

  type Fold[S, E] = (S, E) => Switch[S]


  final case class Switch[@specialized +S](s: S, continue: Boolean) {
    def stop: Boolean = !continue
    def map[SS](f: S => SS): Switch[SS] = copy(s = f(s))
    def enclose: Switch[Switch[S]] = map(_ => this)

    def switch(continue: Boolean = true): Switch[S] = {
      if (continue == /*TODO use Eq*/ this.continue) this
      else copy(continue = continue)
    }
  }

  object Switch {
    val Unit: Switch[Unit] = continue(())

    def continue[@specialized S](s: S) = Switch(s, continue = true)
    def stop[@specialized S](s: S) = Switch(s, continue = false)
  }


  implicit class SwitchIdOps[S](val self: S) extends AnyVal {

    def continue: Switch[S] = Switch.continue(self)

    def stop: Switch[S] = Switch.stop(self)

    def switch(continue: Boolean): Switch[S] = Switch(self, continue)
  }


  implicit class IterableFoldWhile[A](val self: Iterable[A]) extends AnyVal {
    import IterableFoldWhile._

    def foldWhile[S](s: S)(f: Fold[S, A]): Switch[S] = {
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

    def foldWhileM[F[_], L, R](l: L)(f: (L, A) => F[Either[L, R]])(implicit F: Monad[F]): F[Either[L, R]] = {
      Stream[F].apply(self.toList).foldWhileM(l)(f)
    }
  }


  object IterableFoldWhile {
    private final case class Return[S](s: S) extends ControlThrowable
  }
}
