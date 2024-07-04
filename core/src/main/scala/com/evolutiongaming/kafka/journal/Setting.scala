package com.evolutiongaming.kafka.journal

import java.time.Instant

final case class Setting(key: Setting.Key, value: Setting.Value, timestamp: Instant, origin: Option[Origin])

object Setting {
  type Key   = String
  type Value = String
}
