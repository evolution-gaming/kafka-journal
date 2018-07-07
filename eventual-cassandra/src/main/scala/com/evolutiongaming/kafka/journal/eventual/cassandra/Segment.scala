package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.Alias.SeqNr

import scala.collection.immutable.Seq

case class Segment(value: Long) extends Ordered[Segment] {

  //  def next: Segment = copy(value + 1) TODO implement and pass seqNr as argument

  def compare(that: Segment): Int = this.value compare that.value

  def to(segment: Segment): Seq[Segment] = {
    if (this == segment) Seq.empty
    else {
      val range = this.value to segment.value
      range.map { value => Segment(value) }
    }
  }
}

object Segment {
  def apply(seqNr: SeqNr, segmentSize: Int): Segment = {
    val value = (seqNr - 1) / segmentSize
    Segment(value)
  }
}
