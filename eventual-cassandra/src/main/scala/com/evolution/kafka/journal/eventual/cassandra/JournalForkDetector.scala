package com.evolution.kafka.journal.eventual.cassandra

import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
import cats.{Applicative, MonadThrow}
import com.evolution.kafka.journal.eventual.{EventualPayloadAndType, ReplicatedJournal}
import com.evolution.kafka.journal.{EventRecord, Key}
import com.evolutiongaming.catshelper.Log

/**
 * Detects [[JournalFork]]s among the appended events, see [[JournalFork.fromEvents]].
 */
private[journal] trait JournalForkDetector[F[_]] {

  def detect(key: Key, journalHead: Option[JournalHead], events: Nel[EventRecord[EventualPayloadAndType]]): F[Unit]
}

private[journal] object JournalForkDetector {

  def empty[F[_]: Applicative]: JournalForkDetector[F] =
    (_, _, _) => ().pure[F]

  def fromReporter[F[_]: MonadThrow](reporter: JournalForkReporter[F]): JournalForkDetector[F] = {
    (key, journalHead, events) =>
      // defensive on purpose: the reporter only logs and counts today, but whatever it does in
      // future, a failure there must not stop the replication
      MonadThrow[F]
        .catchNonFatal(JournalFork.fromEvents(key, journalHead, events.toList))
        .flatMap { forks => forks.traverse_(reporter.forkSuspected) }
        .handleErrorWith { error => reporter.detectionFailed(key, journalHead, events, error).attempt.void }
  }

  def fromConfig[F[_]: MonadThrow](
    config: ReplicatedJournal.DataIntegrityConfig,
    metrics: ReplicatedJournal.Metrics[F],
    log: Log[F],
  ): JournalForkDetector[F] = {
    if (config.journalForkDetection) {
      val reporter = JournalForkReporter.fromMetrics(metrics, log)
      fromReporter(reporter)
    } else {
      empty
    }
  }
}
