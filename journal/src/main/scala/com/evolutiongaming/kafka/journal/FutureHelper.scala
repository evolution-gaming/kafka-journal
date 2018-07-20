package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.kafka.journal.Alias.SeqNr

import scala.concurrent.Future

object FutureHelper {
  private val futureSeqNr = SeqNr.Min.future


  implicit class FutureSeqNrOps(val self: Future.type) extends AnyVal {
    def seqNr[T]: Future[SeqNr] = futureSeqNr
  }
}
