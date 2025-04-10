package com.evolutiongaming.kafka.journal.util

import com.evolutiongaming.kafka.journal.FromConfigReaderResult
import pureconfig.ConfigReader

import scala.reflect.ClassTag

object PureConfigHelper {

  implicit class ConfigReaderResultOpsPureConfigHelper[A: ClassTag](val self: ConfigReader.Result[A]) {

    def liftTo[F[_]: FromConfigReaderResult]: F[A] = FromConfigReaderResult[F].apply(self)
  }
}
