package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Applicative
import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import com.datastax.driver.core.{Duration => DurationC, _}
import com.evolutiongaming.kafka.journal.eventual.cassandra.util.FiniteDurationHelper._
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}
import com.evolutiongaming.sstream.Stream

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object CassandraHelper {

  implicit class StatementOps(val self: Statement) extends AnyVal {

    def execute[F[_]: CassandraSession]: Stream[F, Row] = CassandraSession[F].execute(self)

    def first[F[_]: CassandraSession: Applicative]: F[Option[Row]] = execute[F].first
  }

  implicit class QueryOps(val self: String) extends AnyVal {

    def prepare[F[_]: CassandraSession]: F[PreparedStatement] =
      CassandraSession[F].prepare(self)

    def execute[F[_]: CassandraSession]: Stream[F, Row] =
      CassandraSession[F].execute(self)
  }

  implicit class RowOps(val self: Row) extends AnyVal {

    /** Same as [[com.datastax.driver.core.ResultSet#wasApplied]] with some minor differences (i.e. it may throw an
      * exception).
      *
      * @throws IllegalArgumentException
      *   if `[applied]` column is not found, i.e. for non-conditional or DDL queries.
      */
    def wasApplied: Boolean = self.decode[Boolean]("[applied]")

  }

  implicit class EncodeRowObjOps(val self: EncodeRow.type) extends AnyVal {

    def empty[A]: EncodeRow[A] = new EncodeRow[A] {

      def apply[B <: SettableData[B]](data: B, value: A) = data
    }

    def noneAsUnset[A](implicit encode: EncodeRow[A]): EncodeRow[Option[A]] = new EncodeRow[Option[A]] {

      def apply[B <: SettableData[B]](data: B, value: Option[A]) =
        value.fold(data)(encode(data, _))
    }
  }

  implicit class DecodeRowObjOps(val self: DecodeRow.type) extends AnyVal {

    def const[A](a: A): DecodeRow[A] = (_: GettableByNameData) => a
  }

  implicit val mapTextEncodeByName: EncodeByName[Map[String, String]] = {
    val text = classOf[String]
    new EncodeByName[Map[String, String]] {
      def apply[B <: SettableData[B]](data: B, name: String, value: Map[String, String]) =
        data.setMap(name, value.asJava, text, text)
    }
  }

  implicit val mapTextDecodeByName: DecodeByName[Map[String, String]] = {
    val text = classOf[String]
    (data: GettableByNameData, name: String) => data.getMap(name, text, text).asScala.toMap
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

  implicit val finiteDurationEncodeByName: EncodeByName[FiniteDuration] =
    EncodeByName[DurationC].contramap(finiteDurationToDuration)

  implicit val finiteDurationDecodeByName: DecodeByName[FiniteDuration] =
    DecodeByName[DurationC].map(durationToFiniteDuration)
}
