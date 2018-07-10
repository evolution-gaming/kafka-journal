package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Alias.{Bytes, SeqNr}

case class Event(seqNr: SeqNr, tags: Set[String], payload: Bytes) {

  // TODO change if case class for bytes
  override def toString: String = {
    val bytes = payload.length
    s"$productPrefix($seqNr,$tags,Bytes($bytes))"
  }
}