package com.evolutiongaming.kafka.journal

import cats.syntax.all.*
import com.evolutiongaming.catshelper.ApplicativeThrowable
import pureconfig.ConfigReader
import pureconfig.error.ConfigReaderException

import scala.reflect.ClassTag

trait FromConfigReaderResult[F[_]] {
  def liftToF[A: ClassTag](result: ConfigReader.Result[A]): F[A]
}

object FromConfigReaderResult {

  def apply[F[_]](
    implicit
    F: FromConfigReaderResult[F],
  ): FromConfigReaderResult[F] = F

  implicit def lift[F[_]: ApplicativeThrowable]: FromConfigReaderResult[F] = {
    new FromConfigReaderResult[F] {
      override def liftToF[A: ClassTag](result: ConfigReader.Result[A]): F[A] = {
        result.fold(failures => ConfigReaderException[A](failures).raiseError[F, A], _.pure[F])
      }
    }
  }
}
