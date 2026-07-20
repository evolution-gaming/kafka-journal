package com.evolution.kafka.journal

import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.evolution.kafka.journal.JournalSpec.*
import com.evolution.kafka.journal.eventual.EventualJournal
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * Worst-case scenarios of journal read (issue #14): exercises [[Journals.read]] over the in-memory
 * model reused from [[JournalSpec]], but at large/pathological scale rather than the size-0..5
 * combinations. Covers a cold head cache, a large non-replicated Kafka tail, a Cassandra/Kafka
 * merge seam, a duplicated (replayed) tail, a deep read offset, many interleaved deletes, and
 * delete/purge. Correctness only; performance is measured by `JournalReadBenchmark` in the
 * `benchmark` module.
 */
class JournalReadWorstCaseSpec extends AnyWordSpec with Matchers {

  private val journalSize = 1000

  private val seqNrs: List[SeqNr] = (1 to journalSize).toList.map { n => SeqNr.unsafe(n) }

  private val headCaches: List[(String, HeadCache[StateT])] = List(
    "cold head cache (cache miss)" -> HeadCache.const(none[HeadInfo].pure[StateT]),
    "warm head cache" -> StateT.headCache,
  )

  "Journal.read worst cases" when {

    for {
      (headCacheName, headCache) <- headCaches
    } {

      headCacheName should {

        "read a large non-replicated Kafka tail (nothing in Cassandra)" in {
          val result = run(eventual = EventualJournal.empty[StateT], headCache = headCache) { journal =>
            appendAll(journal) *> journal.read(SeqRange.all)
          }
          result shouldEqual seqNrs
        }

        "read a large journal split across Cassandra and the Kafka tail (eventual behind)" in {
          val result = run(produceAction = StateT.eventualActionsBehind(journalSize / 2), headCache = headCache) {
            journal =>
              appendAll(journal) *> journal.read(SeqRange.all)
          }
          result shouldEqual seqNrs
        }

        "deduplicate a large replayed Kafka tail (duplicate seqNr)" in {
          val result = run(
            eventual = EventualJournal.empty[StateT],
            consumeActionRecords = StateT.consumeActionRecords.withDuplicates,
            headCache = headCache,
          ) { journal =>
            appendAll(journal) *> journal.read(SeqRange.all)
          }
          result shouldEqual seqNrs
        }

        "read a suffix of a large journal starting from a high SeqNr" in {
          val from = journalSize / 2 + 1
          val result = run(headCache = headCache) { journal =>
            appendAll(journal) *> journal.read(SeqRange(SeqNr.unsafe(from), SeqNr.max))
          }
          result shouldEqual seqNrs.drop(journalSize / 2)
        }

        "read after many interleaved deletes leaves the correct suffix" in {
          val deletes = (1 to journalSize / 2 by 10).toList
          val result = run(headCache = headCache) { journal =>
            for {
              _ <- appendAll(journal)
              _ <- deletes.foldLeftM(()) { (_, n) => journal.delete(SeqNr.unsafe(n).toDeleteTo).void }
              seqNrs <- journal.read(SeqRange.all)
            } yield seqNrs
          }
          result shouldEqual seqNrs.drop(deletes.last)
        }

        "delete-all on a large journal yields an empty read and a pointer at the last SeqNr" in {
          val result = run(headCache = headCache) { journal =>
            for {
              _ <- appendAll(journal)
              _ <- journal.delete(SeqNr.unsafe(journalSize).toDeleteTo)
              seqNrs <- journal.read(SeqRange.all)
              pointer <- journal.pointer
            } yield (seqNrs, pointer)
          }
          result shouldEqual ((List.empty[SeqNr], SeqNr.unsafe(journalSize).some))
        }

        "purge on a large journal yields an empty read and no pointer" in {
          val result = run(headCache = headCache) { journal =>
            for {
              _ <- appendAll(journal)
              _ <- journal.purge
              seqNrs <- journal.read(SeqRange.all)
              pointer <- journal.pointer
            } yield (seqNrs, pointer)
          }
          result shouldEqual ((List.empty[SeqNr], none[SeqNr]))
        }
      }
    }
  }

  private def appendAll(journal: SeqNrJournal[StateT]): StateT[Unit] =
    seqNrs.foldLeftM(()) { (_, seqNr) => journal.append(seqNr).void }

  private def run[A](
    eventual: EventualJournal[StateT] = StateT.eventualJournal,
    consumeActionRecords: ConsumeActionRecords[StateT] = StateT.consumeActionRecords,
    produceAction: ProduceAction[StateT] = StateT.produceAction,
    headCache: HeadCache[StateT],
  )(
    use: SeqNrJournal[StateT] => StateT[A],
  ): A = {
    val journal = SeqNrJournal(eventual, consumeActionRecords, produceAction, headCache)
    use(journal)
      .run(State.empty)
      .map { case (_, a) => a }
      .unsafeRunSync()
  }
}
