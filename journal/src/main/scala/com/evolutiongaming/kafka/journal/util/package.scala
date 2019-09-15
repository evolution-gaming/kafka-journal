package com.evolutiongaming.kafka.journal

import cats.{ApplicativeError, MonadError}

package object util {

  type ApplicativeString[F[_]] = ApplicativeError[F, String]
  
  type MonadString[F[_]] = MonadError[F, String]
}
