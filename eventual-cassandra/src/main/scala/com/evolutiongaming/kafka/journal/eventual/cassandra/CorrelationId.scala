package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.evolutiongaming.scassandra.*
import com.datastax.driver.core.{GettableByNameData, SettableData}

import scala.jdk.CollectionConverters.*

final case class CorrelationId(value: String) extends AnyVal {
  override def toString: String = value
}

object CorrelationId {
  def unsafe(value: String): CorrelationId = new CorrelationId(value)

  def of[F[_]: Monad: UUIDGen]: F[CorrelationId] =
    UUIDGen[F]
      .randomUUID
      .map { uuid =>
        CorrelationId(uuid.toString)
      }

  val key = "correlation_id"

  implicit val encodeByNameCorrelationId: EncodeByName[CorrelationId] = EncodeByName[String].contramap(_.value)
  implicit val decodeByNameCorrelationId: DecodeByName[CorrelationId] = DecodeByName[String].map(CorrelationId.unsafe)
  implicit val encodeByIdxCorrelationId: EncodeByIdx[CorrelationId]   = EncodeByIdx[String].contramap(_.value)
  implicit val decodeByIdxCorrelationId: DecodeByIdx[CorrelationId]   = DecodeByIdx[String].map(CorrelationId.unsafe)

  implicit val encodeAsPropertiesCorrelationId: EncodeRow[Option[CorrelationId]] = new EncodeRow[Option[CorrelationId]] {
    def apply[B <: SettableData[B]](data: B, value: Option[CorrelationId]) = {
      value
        .map {
          case CorrelationId(value) =>
            data.setMap("properties", Map(key -> value).asJava, classOf[String], classOf[String])
        }
        .getOrElse(data)
    }
  }

  implicit val decodePropertiesAsCorrelationId: DecodeRow[Option[CorrelationId]] = new DecodeRow[Option[CorrelationId]] {
    def apply(data: GettableByNameData) = {
      val properties = data.getMap("properties", classOf[String], classOf[String])
      Option(properties.get(key)).map(CorrelationId.unsafe)
    }
  }
}
