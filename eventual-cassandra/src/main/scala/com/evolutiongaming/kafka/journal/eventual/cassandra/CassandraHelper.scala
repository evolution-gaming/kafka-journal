package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.{LocalDate => LocalDateJ}

import cats.Applicative
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.datastax.driver.core._
import com.evolutiongaming.scassandra.{DecodeByIdx, DecodeByName, EncodeByIdx, EncodeByName}
import com.evolutiongaming.sstream.Stream

import scala.collection.JavaConverters._

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


  implicit val localDateEncodeByName: EncodeByName[LocalDateJ] = new EncodeByName[LocalDateJ] {

    def apply[B <: SettableData[B]](data: B, name: String, value: LocalDateJ) = {
      val date = LocalDate.fromDaysSinceEpoch(value.toEpochDay.toInt)
      data.setDate(name, date)
    }
  }

  implicit val localDateDecodeByName: DecodeByName[LocalDateJ] = new DecodeByName[LocalDateJ] {
    def apply(data: GettableByNameData, name: String) = {
      val date = data.getDate(name)
      LocalDateJ.ofEpochDay(date.getDaysSinceEpoch.toLong)
    }
  }


  implicit val localDateEncodeByIdx: EncodeByIdx[LocalDateJ] = new EncodeByIdx[LocalDateJ] {
    def apply[B <: SettableData[B]](data: B, idx: Int, value: LocalDateJ) = {
      val date = LocalDate.fromDaysSinceEpoch(value.toEpochDay.toInt)
      data.setDate(idx, date)
    }
  }


  implicit val localDateDecodeByIdx: DecodeByIdx[LocalDateJ] = new DecodeByIdx[LocalDateJ] {
    def apply(data: GettableByIndexData, idx: Int) = {
      val date = data.getDate(idx)
      LocalDateJ.ofEpochDay(date.getDaysSinceEpoch.toLong)
    }
  }
}