package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.Alias.SeqNr

object AsyncHelper {
  private val asyncSeqNr = SeqNr.Min.async

  implicit class AsyncObjOps(val self: Async.type) extends AnyVal {
    def seqNr[T]: Async[SeqNr] = asyncSeqNr
  }
}
