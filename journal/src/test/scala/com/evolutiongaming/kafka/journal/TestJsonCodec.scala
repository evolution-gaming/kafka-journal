package com.evolutiongaming.kafka.journal

import com.evolutiongaming.catshelper.{ApplicativeThrowable, FromTry}

object TestJsonCodec {

  implicit def instance[F[_] : ApplicativeThrowable : FromTry]: JsonCodec[F] = JsonCodec.default[F]
}
