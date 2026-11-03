package com.evolution.kafka.journal.eventual.cassandra

import cats.Applicative
import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
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

  def fromReporter[F[_]: Applicative](reporter: JournalForkReporter[F]): JournalForkDetector[F] = {
    (key, journalHead, events) =>
      // a failed detection is only reported, so that it does not stop the replication
      Either.catchNonFatal(JournalFork.fromEvents(key, journalHead, events.toList)) match {
        case Right(forks) => forks.traverse_(reporter.forkSuspected)
        case Left(error) => reporter.detectionFailed(key, journalHead, events, error)
      }
  }

  def fromConfig[F[_]: Applicative](
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
