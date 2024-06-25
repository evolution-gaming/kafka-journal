package com.evolutiongaming.kafka.journal.util

import com.evolutiongaming.kafka.journal.FromConfigReaderResult
import pureconfig.ConfigReader

object PureConfigHelper {

  implicit class ConfigReaderResultOpsPureConfigHelper[A](val self: ConfigReader.Result[A]) extends AnyVal {

    def liftTo[F[_]: FromConfigReaderResult]: F[A] = FromConfigReaderResult[F].apply(self)
  }
}
