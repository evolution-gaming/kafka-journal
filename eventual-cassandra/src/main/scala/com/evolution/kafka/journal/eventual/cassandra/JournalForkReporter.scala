package com.evolution.kafka.journal.eventual.cassandra

import cats.Applicative
import cats.syntax.all.*
import com.evolution.kafka.journal.eventual.ReplicatedJournal
import com.evolutiongaming.catshelper.Log

/**
 * Reports a detected [[JournalFork]], e.g. to logs and metrics.
 */
private[journal] trait JournalForkReporter[F[_]] {

  def report(fork: JournalFork): F[Unit]
}

private[journal] object JournalForkReporter {

  def empty[F[_]: Applicative]: JournalForkReporter[F] = _ => Applicative[F].unit

  def fromMetrics[F[_]: Applicative](
    metrics: ReplicatedJournal.Metrics[F],
    log: Log[F],
  ): JournalForkReporter[F] = { fork =>
    // for a proven duplicate, reuse the `seqNr ... duplicated` wording of the recovery error in
    // `EventualCassandra`, so that one search finds both the fork and the recoveries it breaks
    val headline =
      if (fork.duplicateProven) s"Data integrity violated: seqNr ${ fork.seqNr } duplicated by a journal fork"
      else s"Suspected journal fork: seqNr ${ fork.seqNr } did not increase"
    val message = s"$headline, ${ fork.show }"

    val logFork = if (fork.duplicateProven) log.error(message) else log.warn(message)
    val updateMetrics = metrics.journalForkDetected(fork.key.topic, fork.duplicateProven)

    logFork *> updateMetrics
  }
}
