package com.evolutiongaming.cassandra

import java.nio.ByteBuffer
import java.time.Instant
import java.util.Date

import com.datastax.driver.core.BoundStatement

import scala.collection.JavaConverters._

// TODO check performance of binding `by name`
// TODO cover with tests
// TODO add codecs for all supported types
trait Encode[-A] { self =>

  def apply(statement: BoundStatement, name: String, value: A): BoundStatement

  final def imap[B](f: B => A): Encode[B] = new Encode[B] {
    def apply(statement: BoundStatement, name: String, value: B) = self(statement, name, f(value))
  }
}

object Encode {

  def apply[A](implicit encode: Encode[A]): Encode[A] = encode

  implicit def opt[A](implicit encode: Encode[A]): Encode[Option[A]] = new Encode[Option[A]] {
    def apply(statement: BoundStatement, name: String, value: Option[A]) = {
      value match {
        case Some(value) => encode(statement, name, value)
        case None        => statement.setToNull(name)
      }
    }
  }

  implicit val StrImpl: Encode[String] = new Encode[String] {
    def apply(statement: BoundStatement, name: String, value: String) = statement.setString(name, value)
  }

  implicit val StrOptImpl: Encode[Option[String]] = opt[String]

  implicit val IntImpl: Encode[Int] = new Encode[Int] {
    def apply(statement: BoundStatement, name: String, value: Int) = statement.setInt(name, value)
  }

  implicit val IntOptImpl: Encode[Option[Int]] = opt[Int]

  implicit val LongImpl: Encode[Long] = new Encode[Long] {
    def apply(statement: BoundStatement, name: String, value: Long) = statement.setLong(name, value)
  }

  implicit val LongOptImpl: Encode[Option[Long]] = opt[Long]

  implicit val InstantImpl: Encode[Instant] = new Encode[Instant] {
    def apply(statement: BoundStatement, name: String, value: Instant) = {
      val timestamp = Date.from(value)
      statement.setTimestamp(name, timestamp)
    }
  }

  implicit val InstantOptImpl: Encode[Option[Instant]] = opt[Instant]

  implicit val SetStrImpl: Encode[Set[String]] = new Encode[Set[String]] {
    def apply(statement: BoundStatement, name: String, value: Set[String]) = {
      val set = value.asJava
      statement.setSet(name, set, classOf[String])
    }
  }

  implicit val BytesImpl: Encode[Array[Byte]] = new Encode[Array[Byte]] {
    def apply(statement: BoundStatement, name: String, value: Array[Byte]) = {
      val bytes = ByteBuffer.wrap(value)
      statement.setBytes(name, bytes)
    }
  }
}

