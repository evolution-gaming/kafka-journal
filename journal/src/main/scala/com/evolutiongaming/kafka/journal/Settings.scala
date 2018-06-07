package com.evolutiongaming.kafka.journal

import com.typesafe.config.Config

case class Settings()

object Settings {
  def apply(config: Config): Settings = ???
}
