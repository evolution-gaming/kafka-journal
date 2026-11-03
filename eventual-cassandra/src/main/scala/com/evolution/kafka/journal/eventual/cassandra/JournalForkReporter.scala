package com.evolution.kafka.journal.eventual.cassandra

import cats.Applicative
import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
import com.evolution.kafka.journal.eventual.{EventualPayloadAndType, ReplicatedJournal}
import com.evolution.kafka.journal.{EventRecord, Key}
import com.evolutiongaming.catshelper.Log

/**
 * Reports suspected [[JournalFork]]s and failed detections, e.g. to logs and metrics.
 */
private[journal] trait JournalForkReporter[F[_]] {

  /**
   * Reports a suspected [[JournalFork]], either a `duplicate` or a `regression`.
   */
  def forkSuspected(fork: JournalFork): F[Unit]

  /**
   * Reports that the detection failed, i.e. forks among the appended events, if any, were not
   * detected.
   */
  def detectionFailed(
    key: Key,
    journalHead: Option[JournalHead],
    events: Nel[EventRecord[EventualPayloadAndType]],
    error: Throwable,
  ): F[Unit]
}

private[journal] object JournalForkReporter {

  def empty[F[_]: Applicative]: JournalForkReporter[F] = new JournalForkReporter[F] {

    def forkSuspected(fork: JournalFork): F[Unit] = ().pure[F]

    def detectionFailed(
      key: Key,
      journalHead: Option[JournalHead],
      events: Nel[EventRecord[EventualPayloadAndType]],
      error: Throwable,
    ): F[Unit] =
      ().pure[F]
  }

  def fromMetrics[F[_]: Applicative](
    metrics: ReplicatedJournal.Metrics[F],
    log: Log[F],
  ): JournalForkReporter[F] = new JournalForkReporter[F] {

    def forkSuspected(fork: JournalFork): F[Unit] = {
      // for a proven duplicate, reuse the `seqNr ... duplicated` wording of the recovery error in
      // `EventualCassandra`, so that one search finds both the fork and the recoveries it breaks
      val headline =
        if (fork.isDuplicate) s"Data integrity violated: seqNr ${ fork.detectedAtSeqNr } duplicated by a journal fork"
        else s"Suspected journal fork: seqNr ${ fork.detectedAtSeqNr } did not increase"
      val message = s"$headline, ${ fork.show }"

      val logFork = if (fork.isDuplicate) log.error(message) else log.warn(message)
      val updateMetrics = metrics.journalFork(fork.key.topic, fork.isDuplicate)

      logFork *> updateMetrics
    }

    def detectionFailed(
      key: Key,
      journalHead: Option[JournalHead],
      events: Nel[EventRecord[EventualPayloadAndType]],
      error: Throwable,
    ): F[Unit] = {
      val head = journalHead.fold("none") { journalHead =>
        s"${ journalHead.partitionOffset } (seqNr ${ journalHead.seqNr })"
      }
      val offsets = s"${ events.head.partitionOffset }..${ events.last.partitionOffset }"
      log.error(s"Journal fork detection failed, key: $key, head: $head, offsets: $offsets, error: $error", error)
    }
  }
}
