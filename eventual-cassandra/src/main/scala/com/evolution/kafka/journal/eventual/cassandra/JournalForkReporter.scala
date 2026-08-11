package com.evolution.kafka.journal.eventual.cassandra

import cats.Applicative
import cats.syntax.all.*
import com.evolution.kafka.journal.eventual.ReplicatedJournal
import com.evolutiongaming.catshelper.Log

/**
 * Where a detected [[JournalFork]] goes.
 */
private[journal] trait JournalForkReporter[F[_]] {

  def report(fork: JournalFork): F[Unit]
}

private[journal] object JournalForkReporter {

  /**
   * Reports nowhere, for a journal which is not watched for forks.
   */
  def empty[F[_]: Applicative]: JournalForkReporter[F] = _ => Applicative[F].unit

  /**
   * `ERROR` for a proven duplicate, `WARN` for a bare regression, and a `topic` and `kind` labelled
   * counter either way.
   */
  def apply[F[_]: Applicative](
    log: Log[F],
    metrics: Option[ReplicatedJournal.Metrics[F]],
  ): JournalForkReporter[F] = { fork =>
    // the headline repeats the `seqNr ... duplicated` wording of the recovery side error, so that a
    // single grep finds both the fork and the recoveries it later breaks - but only where the
    // duplicate is proven, so that the phrase keeps meaning what it says
    val headline =
      if (fork.duplicateProven) s"Data integrity violated: seqNr ${ fork.seqNr } duplicated by a journal fork"
      else s"Suspected journal fork: seqNr ${ fork.seqNr } did not increase"
    val message = s"$headline, ${ fork.show }"

    val logFork = if (fork.duplicateProven) log.error(message) else log.warn(message)

    val updateMetrics = metrics.traverse_ { metrics =>
      metrics.journalForkDetected(fork.key.topic, fork.duplicateProven)
    }

    logFork *> updateMetrics
  }
}
