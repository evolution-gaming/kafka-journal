package com.evolutiongaming.kafka.journal

import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.kafka.journal.util.Fail
import scodec.bits.ByteVector

object ByteVectorOf {

  def apply[F[_] : Monad : Fail](clazz: Class[_], path: String): F[ByteVector] = {
    for {
      is <- Option(clazz.getResourceAsStream(path)).getOrError[F](s"file not found at $path")
    } yield {
      val bytes = new Array[Byte](is.available())
      is.read(bytes)
      ByteVector.view(bytes)
    }
  }
}
