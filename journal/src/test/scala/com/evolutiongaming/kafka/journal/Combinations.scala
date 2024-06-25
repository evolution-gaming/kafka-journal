package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}

object Combinations {

  type Type[T] = List[List[Nel[T]]]

  object Type {
    private val Empty: Type[Nothing] = List(Nil)

    def empty[T]: Type[T] = Empty
  }

  def apply[T](xs: List[T]): Type[T] = {

    def loop(xs: List[T]): Type[T] = xs match {
      case Nil => List(Nil)
      case head :: tail =>
        val xxs = for {
          ts <- loop(tail)
          a   = Nel.of(head) :: ts
          bs  = ts.headOption.fold[Type[T]](Nil)(ht => List((head :: ht) :: ts.tail))
          x  <- a :: bs
        } yield x
        List(Nel(head, tail)) :: xxs
    }

    loop(xs).distinct
  }
}
