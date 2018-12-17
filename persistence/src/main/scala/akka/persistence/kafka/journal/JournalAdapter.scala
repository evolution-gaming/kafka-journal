package akka.persistence.kafka.journal

import java.time.Instant

import akka.persistence.journal.Tagged
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

trait JournalAdapter {

  def write(messages: Seq[AtomicWrite]): Future[List[Try[Unit]]]

  def delete(persistenceId: String, to: SeqNr): Future[Unit]

  def lastSeqNr(persistenceId: String, from: SeqNr): Future[Option[SeqNr]]

  def replay(persistenceId: String, range: SeqRange, max: Long)(f: PersistentRepr => Unit): Future[Unit]
}

object JournalAdapter {

  def apply(
    log: ActorLog,
    toKey: ToKey,
    journal: Journal[Async],
    serializer: EventSerializer)(implicit ec: ExecutionContext): JournalAdapter = new JournalAdapter {

    def write(atomicWrites: Seq[AtomicWrite]) = {
      val timestamp = Instant.now()
      Future {
        val persistentReprs = for {
          atomicWrite <- atomicWrites
          persistentRepr <- atomicWrite.payload
        } yield {
          persistentRepr
        }
        if (persistentReprs.isEmpty) Future.nil
        else {
          val persistenceId = persistentReprs.head.persistenceId
          val key = toKey(persistenceId)

          log.debug {
            val first = persistentReprs.head.sequenceNr
            val last = persistentReprs.last.sequenceNr
            val seqNr = if (first == last) s"seqNr: $first" else s"seqNrs: $first..$last"
            s"$persistenceId write, $seqNr"
          }

          val events = for {
            persistentRepr <- persistentReprs
          } yield {
            serializer.toEvent(persistentRepr)
          }
          val nel = Nel(events.head, events.tail.toList)
          val result = journal.append(key, nel, timestamp)
          result.map(_ => Nil).future
        }
      }.flatten
    }

    def delete(persistenceId: PersistenceId, to: SeqNr) = {
      log.debug(s"$persistenceId delete, to: $to")

      val timestamp = Instant.now()
      val key = toKey(persistenceId)
      journal.delete(key, to, timestamp).unit.future
    }

    def replay(persistenceId: PersistenceId, range: SeqRange, max: Long)
      (callback: PersistentRepr => Unit): Future[Unit] = {

      log.debug(s"$persistenceId replay, range: $range")

      val key = toKey(persistenceId)
      val fold: Fold[Long, Event] = (count, event) => {
        val seqNr = event.seqNr
        if (seqNr <= range.to && count < max) {
          val persistentRepr = serializer.toPersistentRepr(persistenceId, event)
          callback(persistentRepr)
          val countNew = count + 1
          countNew switch countNew != max
        } else {
          count.stop
        }
      }
      val async = journal.read(key, range.from, 0l)(fold)
      async.unit.future
    }

    def lastSeqNr(persistenceId: PersistenceId, from: SeqNr) = {
      log.debug(s"$persistenceId lastSeqNr, from: $from")

      val key = toKey(persistenceId)
      val pointer = for {
        pointer <- journal.pointer(key)
      } yield for {
        pointer <- pointer
        if pointer >= from
      } yield {
        pointer
      }

      pointer.future
    }
  }
}

object PayloadAndTags {
  def apply(payload: Any): (Any, Tags) = payload match {
    case Tagged(payload, tags) => (payload, tags)
    case _                     => (payload, Set.empty)
  }
}

