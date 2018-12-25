package com.evolutiongaming.kafka.journal

import scala.util.control.ControlThrowable

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
}
