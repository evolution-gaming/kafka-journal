package com.evolution.kafka.journal.eventual.cassandra

import cats.Applicative
import cats.syntax.all.*
import com.evolution.kafka.journal.eventual.ReplicatedJournal
import com.evolutiongaming.catshelper.Log

/**
 * Where a detected [[JournalFork]] goes. Kept apart from [[JournalFork]] itself, which is data and
 * a pure derivation over it, so that neither logging nor the metrics SPI reaches into that.
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
   * `ERROR` for a fork which will break a recovery of the entity, `WARN` for one which is only
   * suspected or already harmless, and a `topic` and `consequence` labelled counter either way.
   */
  def apply[F[_]: Applicative](
    log: Log[F],
    metrics: Option[ReplicatedJournal.Metrics[F]],
  ): JournalForkReporter[F] = { fork =>
    val consequence = fork.consequence

    val logFork =
      if (consequence == JournalFork.Consequence.BreaksRecovery) log.error(fork.show)
      else log.warn(fork.show)

    val updateMetrics = metrics.traverse_ { metrics =>
      metrics.journalForkDetected(fork.key.topic, consequence.name)
    }

    logFork *> updateMetrics
  }
}
