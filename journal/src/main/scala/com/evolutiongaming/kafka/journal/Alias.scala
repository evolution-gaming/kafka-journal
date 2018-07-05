package com.evolutiongaming.kafka.journal

object Alias {
  type Timestamp = Long // TODO Instant ?
  type PersistenceId = String
  type Id = String

  //TODO make a case class ?
  type SeqNr = Long
  object SeqNr {
    val Max: SeqNr = Long.MaxValue
    val Min: SeqNr = 0L
  }

  type Bytes = Array[Byte]
  type Tag = String
  type Tags = Set[String]

  implicit class SeqNrOps(val self: SeqNr) extends AnyVal {
    def next: SeqNr = self + 1
  }
}
