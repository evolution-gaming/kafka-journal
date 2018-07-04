package com.evolutiongaming.kafka.journal

import scala.annotation.tailrec

//import scala.collection.immutable.Iterable

object CollectionHelper {

  implicit class IterableOps[T](val self: Iterable[T]) extends AnyVal {

    def foldLeftTODO[S, E](s: S)(f: (S, T) => (Option[S], Iterable[E])): (Option[S], Iterable[E]) = {
      val builder = Iterable.newBuilder[E]

      val ss = self.foldLeft[Option[S]](Some(s)) { (s, a) =>
        s.flatMap { s =>
          val (ss, cc) = f(s, a)
          builder ++= cc
          ss
        }
      }
      val es = builder.result()
      (ss, es)
    }
  }
}
