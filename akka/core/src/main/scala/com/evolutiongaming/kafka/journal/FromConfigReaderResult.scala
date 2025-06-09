package com.evolutiongaming.kafka.journal

import cats.syntax.all.*
import com.evolutiongaming.catshelper.ApplicativeThrowable
import pureconfig.ConfigReader
import pureconfig.ConfigReader.Result
import pureconfig.error.ConfigReaderException

import scala.annotation.nowarn
import scala.reflect.ClassTag

trait FromConfigReaderResult[F[_]] {

  // TODO: [5.0.0 release] remove
  // package-private hidden method to satisfy bincompat
  @deprecated("use liftToF which requires ClassTag[A] for correct rendering of parse errors", since = "4.3.0")
  private[journal] def apply[A](a: ConfigReader.Result[A]): F[A]

  @nowarn // suppressing warnings for the bincompat-mandated default impl
  def liftToF[A: ClassTag](result: ConfigReader.Result[A]): F[A] = {
    // an adequate default impl to satisfy bincompat requirements
    // TODO: [5.0.0 release] remove
    apply[A](result)
  }
}

object FromConfigReaderResult {

  def apply[F[_]](
    implicit
    F: FromConfigReaderResult[F],
  ): FromConfigReaderResult[F] = F

  implicit def lift[F[_]: ApplicativeThrowable]: FromConfigReaderResult[F] = {
    new FromConfigReaderResult[F] {
      override private[journal] def apply[A](result: Result[A]): F[A] = {
        // ~ replicating old behavior without ClassTag[A]
        result.fold(failures => ConfigReaderException[Object](failures).raiseError[F, A], _.pure[F])
      }

      override def liftToF[A: ClassTag](result: ConfigReader.Result[A]): F[A] = {
        result.fold(failures => ConfigReaderException[A](failures).raiseError[F, A], _.pure[F])
      }
    }
  }
}
