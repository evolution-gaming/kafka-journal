package com.evolutiongaming.kafka.journal.util

import com.evolutiongaming.kafka.journal.FromConfigReaderResult
import pureconfig.ConfigReader

import scala.reflect.ClassTag

object PureConfigHelper {

  implicit class ConfigReaderResultOpsPureConfigHelper[A](val self: ConfigReader.Result[A]) extends AnyVal {

    def liftTo[F[_]: FromConfigReaderResult](implicit ct: ClassTag[A]): F[A] = FromConfigReaderResult[F].apply(self)
  }
}
