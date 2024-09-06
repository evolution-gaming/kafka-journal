package com.evolutiongaming.kafka.journal

import cats.Monad
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.datastax.driver.core.{GettableByNameData, SettableData}
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

  implicit val correlationIdOptionDecodeByName: DecodeByName[Option[CorrelationId]] = new DecodeByName[Option[CorrelationId]] {
    def apply(data: GettableByNameData, name: String): Option[CorrelationId] = {
      for {
        map <- Option { data.getMap(name, classOf[String], classOf[String]) }
        cid <- map.asScala.get(key)
      } yield CorrelationId.unsafe(cid)
    }
  }

  implicit val correlationIdOptionUpdateByName: UpdateByName[Option[CorrelationId]] = new UpdateByName[Option[CorrelationId]] {
    def apply[D <: GettableByNameData & SettableData[D]](data: D, name: String, value: Option[CorrelationId]): D = {
      val data1 = for {
        cid <- value
        map <- Option { data.getMap(name, classOf[String], classOf[String]) }
      } yield {
        val updated = map.asScala.toMap.updated(key, cid.value).asJava
        data.setMap(name, updated, classOf[String], classOf[String])
      }
      data1 getOrElse data
    }
  }

}
