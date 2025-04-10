package com.evolutiongaming.kafka.journal.util

import com.evolutiongaming.kafka.journal.FromConfigReaderResult
import pureconfig.ConfigReader

import scala.reflect.ClassTag

object PureConfigHelper {

  implicit class ConfigReaderResultOpsPureConfigHelper[A](val self: ConfigReader.Result[A]) extends AnyVal {

    // TODO: [5.0.0 release] remove
    // package-private hidden method to satisfy bincompat
    @deprecated(
      "use liftTo version with ClassTag[A] requirement for correct rendering of parse errors",
      since = "4.3.0",
    )
    private[util] def liftTo[F[_]: FromConfigReaderResult]: F[A] = FromConfigReaderResult[F].apply(self)

    def liftTo[F[_]: FromConfigReaderResult](
      implicit
      resultValueClassTag: ClassTag[A],
    ): F[A] = FromConfigReaderResult[F].liftToF(self)
  }
}
