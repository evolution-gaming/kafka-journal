package com.evolutiongaming.kafka.journal.util

import cats.effect.Resource
import com.evolutiongaming.catshelper.MonadThrowable

import scala.annotation.nowarn

private[journal] object ResourceHelper {

  implicit final class ResourceOpsResourceHelper[F[_], A](val self: Resource[F, A]) extends AnyVal {

    /** `adaptError` for [[Resource]] requiring only `MonadThrowable[F]`.
      *
      * Since cats-effect 3.6 the implicit `MonadError[Resource[F, *], Throwable]` instance requires
      * `MonadCancel[F, Throwable]`, while the `MonadError`-based one became non-implicit and deprecated.
      * It is used explicitly here to keep binary compatible `enhanceError` signatures, which only require
      * `MonadThrowable[F]`.
      */
    // TODO: remove in the next major release: make `enhanceError` require `BracketThrowable[F]` and use `Resource#adaptError`
    @nowarn("cat=deprecation")
    def adaptErrorResource(pf: PartialFunction[Throwable, Throwable])(implicit F: MonadThrowable[F]): Resource[F, A] =
      Resource.catsEffectMonadErrorForResource(F).adaptError(self)(pf)
  }
}
