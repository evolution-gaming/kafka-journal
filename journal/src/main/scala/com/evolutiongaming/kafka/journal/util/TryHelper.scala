package com.evolutiongaming.kafka.journal.util

import cats.MonadError
import cats.implicits._

import scala.util.Try

object TryHelper {

  implicit val throwableToStr: ToStr[Throwable] = _.getMessage

  implicit val tryMonadString: MonadError[Try, String] = MonadString[Try, Throwable]
}
