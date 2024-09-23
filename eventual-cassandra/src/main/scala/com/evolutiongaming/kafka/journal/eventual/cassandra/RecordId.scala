package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.Sync
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.datastax.driver.core.{GettableByNameData, SettableData}
import com.evolutiongaming.scassandra.*

import java.util.UUID

/**
  * ID that represents unique record _instance_ in a Cassandra table.
  * 
  * Each table record has a private key that is used to identify the record in the table.
  * The record can be later deleted and inserted again, so same private key is used.
  * [[RecordId]] is used to differentiate between different record instances, so
  * that deleted record and recreated one are not considered the same while sharing
  * same private key.
  */
final case class RecordId(value: UUID) extends AnyVal {
  override def toString: String = value.toString()
}

object RecordId {

  private[cassandra] def unsafe: RecordId = new RecordId(UUID.randomUUID())

  def of[F[_]: Sync: UUIDGen]: F[RecordId] =
    Sync[F].defer {
      UUIDGen[F]
        .randomUUID
        .map { uuid => RecordId(uuid) }
    }

  implicit val recordIdDecodeByName: DecodeByName[RecordId] = new DecodeByName[RecordId] {
    override def apply(data: GettableByNameData, name: String): RecordId = RecordId(data.getUUID(name))
  }
  implicit val recordIdEncodeByName: EncodeByName[RecordId] = new EncodeByName[RecordId] {
    override def apply[B <: SettableData[B]](data: B, name: String, recordId: RecordId): B = data.setUUID(name, recordId.value)
  }
}
