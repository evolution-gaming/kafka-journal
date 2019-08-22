package com.evolutiongaming.kafka.journal.util

import cats.data.{NonEmptyList => Nel}
import scodec.{Attempt, Codec, Err}

object ScodecHelper {

  def nelCodec[A](codec: Codec[List[A]]): Codec[Nel[A]] = {
    val to = (a: List[A]) => {
      val nel = Nel.fromList(a)
      Attempt.fromOption(nel, Err("list is empty"))
    }
    val from = (a: Nel[A]) => Attempt.successful(a.toList)
    codec.exmap(to, from)
  }
}
