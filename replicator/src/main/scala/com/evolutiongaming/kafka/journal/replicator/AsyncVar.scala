package com.evolutiongaming.kafka.journal.replicator

import com.evolutiongaming.concurrent
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.concurrent.serially.SeriallyAsync

import scala.concurrent.ExecutionContext

trait AsyncVar[S] {

  def value(): S

  def async[SS](f: S => Async[(S, SS)]): Async[SS]

  def updateAsync(f: S => Async[S]): Async[S] = async { s => f(s).map { s => (s, s) } }

  def apply[SS](f: S => (S, SS)): Async[SS] = async { s => f(s).async }

  def update(f: S => S): Async[S] = apply { s =>
    val ss = f(s)
    (ss, ss)
  }
}

object AsyncVar {

  def apply[S](s: S, serially: SeriallyAsync)(implicit ec: ExecutionContext): AsyncVar[S] = {
    val asyncVar = concurrent.serially.AsyncVar(s, serially)
    apply(asyncVar)
  }

  def apply[S](asyncVar: concurrent.serially.AsyncVar[S])(implicit ec: ExecutionContext): AsyncVar[S] = new AsyncVar[S] {
    def value(): S = asyncVar.value()
    def async[SS](f: S => Async[(S, SS)]): Async[SS] = {
      asyncVar.async { s => f(s).future }.async
    }
  }
}