package com.evolutiongaming.kafka.journal.util

import cats.implicits._

// TODO expiry: remove
object OptionHelper {

  implicit val optionToStr: ToStr[Unit] = _ => ""

  implicit val optionFromStr: FromStr[Unit] = _ => ()

  implicit val optionMonadString: MonadString[Option] = MonadString[Option, Unit]
}
