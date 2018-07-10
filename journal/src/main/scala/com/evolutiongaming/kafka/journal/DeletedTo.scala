package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Alias.SeqNr

sealed trait DeletedTo {
  def seqNr: SeqNr
}

object DeletedTo {
  // TODO maybe rename
  case class Dangling(seqNr: SeqNr) extends DeletedTo

  case class Confirmed(seqNr: SeqNr) extends DeletedTo
}
