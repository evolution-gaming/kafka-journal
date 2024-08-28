package com.evolutiongaming.kafka.journal

import cats.syntax.all.*
import com.evolutiongaming.catshelper.ApplicativeThrowable
import pureconfig.ConfigReader
import pureconfig.error.ConfigReaderException

import scala.reflect.ClassTag

trait FromConfigReaderResult[F[_]] {

  def apply[A: ClassTag](a: ConfigReader.Result[A]): F[A]
}

object FromConfigReaderResult {

  def apply[F[_]](implicit F: FromConfigReaderResult[F]): FromConfigReaderResult[F] = F

  implicit def lift[F[_]: ApplicativeThrowable]: FromConfigReaderResult[F] = {
    new FromConfigReaderResult[F] {
      def apply[A: ClassTag](a: ConfigReader.Result[A]): F[A] = {
        a.fold(a => ConfigReaderException(a).raiseError[F, A], _.pure[F])
      }
    }
  }
}
