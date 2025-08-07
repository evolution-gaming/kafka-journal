package com.evolution.kafka.journal.eventual.cassandra

import cats.effect.Sync
import cats.effect.std.{SecureRandom, UUIDGen}
import cats.syntax.all.*
import com.datastax.driver.core.{GettableByNameData, SettableData}
import com.evolutiongaming.scassandra.*

import java.util.UUID

/**
 * ID that represents unique record _instance_ in a Cassandra table.
 *
 * Each table record has a private key that is used to identify the record in the table. The record
 * can be later deleted and inserted again, so the same private key is used. [[RecordId]] is used to
 * differentiate between different record instances, so that deleted record and recreated one are
 * not considered the same while sharing same private key.
 */
private[journal] final case class RecordId(value: UUID) extends AnyVal {
  override def toString: String = value.toString
}

private[journal] object RecordId {

  private[cassandra] def unsafe: RecordId = new RecordId(UUID.randomUUID())

  def of[F[_]: Sync: SecureRandom]: F[RecordId] =
    Sync[F].defer {
      UUIDGen.fromSecureRandom[F].randomUUID.map(RecordId.apply)
    }

  implicit val recordIdDecodeByName: DecodeByName[RecordId] = new DecodeByName[RecordId] {
    override def apply(data: GettableByNameData, name: String): RecordId = RecordId(data.getUUID(name))
  }
  implicit val recordIdEncodeByName: EncodeByName[RecordId] = new EncodeByName[RecordId] {
    override def apply[B <: SettableData[B]](data: B, name: String, recordId: RecordId): B =
      data.setUUID(name, recordId.value)
  }
}
