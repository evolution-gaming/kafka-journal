package com.evolutiongaming.kafka.journal

object FoldWhile {

  type Fold[S, E] = (S, E) => Switch[S]

  final case class Switch[@specialized +S](s: S, continue: Boolean) {
    def stop: Boolean = !continue
    def map[SS](f: S => SS): Switch[SS] = copy(s = f(s))
    def enclose: Switch[Switch[S]] = map(_ => this)

    def switch(continue: Boolean = true): Switch[S] = {
      if (continue == this.continue) this
      else copy(continue = continue)
    }
  }

  object Switch {
    val Unit: Switch[Unit] = continue(())

    def continue[@specialized S](s: S) = Switch(s, continue = true)
    def stop[@specialized S](s: S) = Switch(s, continue = false)
  }


  implicit class IdSwitchOps[S](val self: S) extends AnyVal {

    def continue: Switch[S] = Switch.continue(self)

    def stop: Switch[S] = Switch.stop(self)

    def switch(continue: Boolean): Switch[S] = Switch(self, continue)
  }
}
