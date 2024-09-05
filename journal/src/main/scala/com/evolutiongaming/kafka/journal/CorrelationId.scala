package com.evolutiongaming.kafka.journal

import cats.Monad
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.datastax.driver.core.{GettableData, SettableData}
import com.evolutiongaming.scassandra.*

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

  implicit val correlationIdCodec: CodecRow[Option[CorrelationId]] = new CodecRow[Option[CorrelationId]] {

    val field = "properties"
    val clazz = classOf[String]

    override def encode[D <: GettableData with SettableData[D]](data: D, value: Option[CorrelationId]): D = {
      value.fold(data) {
        case CorrelationId(value) =>
          val properties = asProperties(data).updated(key, value).asJava
          data.setMap(field, properties, clazz, clazz)
      }
    }

    override def decode[D <: GettableData](data: D): Option[CorrelationId] = {
      val properties = asProperties(data)
      properties.get(key).map(CorrelationId.unsafe)
    }

    private def asProperties[D <: GettableData](data: D): Map[String, String] = {
      val nullable = data.getMap(field, clazz, clazz)
      Option(nullable).map(_.asScala.toMap).getOrElse(Map.empty)
    }

  }

}
