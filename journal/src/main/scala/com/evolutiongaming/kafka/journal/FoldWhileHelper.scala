package com.evolutiongaming.kafka.journal

import cats.{Eval, Foldable}
import com.evolutiongaming.nel.Nel

object FoldWhileHelper {

  implicit val FoldableNel: Foldable[Nel] = new Foldable[Nel] {

    def foldLeft[A, B](fa: Nel[A], b: B)(f: (B, A) => B) = fa.foldLeft(b)(f)

    def foldRight[A, B](fa: Nel[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]) = fa.foldRight(lb)(f)
  }
}
