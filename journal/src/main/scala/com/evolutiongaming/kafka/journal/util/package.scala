package com.evolutiongaming.kafka.journal

import cats.MonadError

package object util {

  type MonadString[F[_]] = MonadError[F, String]
}
