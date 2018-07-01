package com.evolutiongaming.kafka.journal.ally.cassandra

import com.evolutiongaming.kafka.journal.Alias.SeqNr

case class Segment(value: Long) extends Ordered[Segment] {

//  def next: Segment = copy(value + 1)

  def compare(that: Segment): Int = this.value compare that.value
}

object Segment {
  def apply(seqNr: SeqNr, segmentSize: Int): Segment = {
    val value = (seqNr - 1) / segmentSize
    Segment(value)
  }
}
