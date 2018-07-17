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

  type Tag = String


  type Tags = Set[String]

  object Tags {
    val Empty: Tags = Set.empty
  }


  implicit class SeqNrOps(val self: SeqNr) extends AnyVal {

    def next: SeqNr = self + 1

    def prev: SeqNr = self - 1

    def in(range: SeqRange): Boolean = range contains self

    def __(seqNr: SeqNr): SeqRange = SeqRange(self, seqNr)

    def __ : SeqRange = SeqRange(self)
  }
}
