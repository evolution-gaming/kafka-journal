package com.evolutiongaming.kafka.journal

import cats.Applicative
import com.evolutiongaming.catshelper.FromTry

object TestJsonCodec {

  implicit def instance[F[_] : Applicative : FromTry]: JsonCodec[F] = JsonCodec.playJson
}
