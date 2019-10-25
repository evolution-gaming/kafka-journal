package com.evolutiongaming.kafka.journal.eventual.cassandra


import cats.Applicative
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.datastax.driver.core.{Duration => DurationC, _}
import com.evolutiongaming.kafka.journal.eventual.cassandra.util.FiniteDurationHelper._
import com.evolutiongaming.scassandra.{DecodeByName, EncodeByName}
import com.evolutiongaming.sstream.Stream

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object CassandraHelper {

  implicit class StatementOps(val self: Statement) extends AnyVal {

    def execute[F[_] : CassandraSession]: Stream[F, Row] = CassandraSession[F].execute(self)

    def first[F[_] : CassandraSession : Applicative]: F[Option[Row]] = execute[F].first
  }


  implicit class QueryOps(val self: String) extends AnyVal {

    def prepare[F[_] : CassandraSession]: F[PreparedStatement] = {
      CassandraSession[F].prepare(self)
    }

    def execute[F[_] : CassandraSession]: Stream[F, Row] = {
      CassandraSession[F].execute(self)
    }
  }


  implicit val mapTextEncodeByName: EncodeByName[Map[String, String]] = {
    val text = classOf[String]
    new EncodeByName[Map[String, String]] {
      def apply[B <: SettableData[B]](data: B, name: String, value: Map[String, String]) = {
        data.setMap(name, value.asJava, text, text)
      }
    }
  }

  implicit val mapTextDecodeByName: DecodeByName[Map[String, String]] = {
    val text = classOf[String]
    (data: GettableByNameData, name: String) => {
      data.getMap(name, text, text).asScala.toMap
    }
  }


  implicit val nelIntEncodeByName: EncodeByName[Nel[Int]] = {
    val integer = classOf[Integer]
    new EncodeByName[Nel[Int]] {
      def apply[B <: SettableData[B]](data: B, name: String, value: Nel[Int]) = {
        val values = value.distinct.toList.map(a => a: Integer).asJava
        data.setList(name, values, integer)
      }
    }
  }


  implicit val finiteDurationEncodeByName: EncodeByName[FiniteDuration] = {
    EncodeByName[DurationC].contramap(finiteDurationToDuration)
  }

  implicit val finiteDurationDecodeByName: DecodeByName[FiniteDuration] = {
    DecodeByName[DurationC].map(durationToFiniteDuration)
  }
}