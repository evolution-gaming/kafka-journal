package com.evolutiongaming.kafka.journal.util

import cats.implicits._
import com.evolutiongaming.kafka.journal.JournalError

import scala.util.Try

// TODO expiry: remove
object TryHelper {

  implicit val throwableToStr: ToStr[Throwable] = _.getMessage

  implicit val throwableFromStr: FromStr[Throwable] = JournalError(_)

  implicit val tryMonadString: MonadString[Try] = MonadString[Try, Throwable]
}
