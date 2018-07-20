package akka.persistence.kafka.journal

import java.time.Instant

import akka.persistence.journal.Tagged
import akka.persistence.{AtomicWrite, PersistentRepr}
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.FutureHelper._
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.{Bytes, Event, Journals}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.serialization.{SerializedMsg, SerializedMsgConverter}

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

trait JournalsAdapter {
  def write(messages: Seq[AtomicWrite]): Future[List[Try[Unit]]]
  def delete(persistenceId: String, to: SeqNr): Future[Unit]
  def highestSeqNr(persistenceId: String, from: SeqNr): Future[Long]
  def replay(persistenceId: String, from: SeqNr, to: SeqNr, max: Long)(f: PersistentRepr => Unit): Future[Unit]
}

object JournalsAdapter {

  def apply(
    log: ActorLog,
    toKey: ToKey,
    journals: Journals,
    serialisation: SerializedMsgConverter)(implicit ec: ExecutionContext): JournalsAdapter = {

    new JournalsAdapter {
      def write(atomicWrites: Seq[AtomicWrite]) = {
        val timestamp = Instant.now()
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

          def seqNrs = persistentReprs.map(_.sequenceNr).mkString(",")

          log.debug(s"asyncWriteMessages persistenceId: $persistenceId, seqNrs: $seqNrs")

          val async = Async.async {
            val events = for {
              persistentRepr <- persistentReprs
            } yield {
              val (payload: AnyRef, tags) = PayloadAndTags(persistentRepr.payload)
              val serialized = serialisation.toMsg(payload)
              val persistentEvent = PersistentEvent(serialized, persistentRepr)
              val bytes = PersistentEventSerializer.toBinary(persistentEvent)
              Event(persistentRepr.sequenceNr, tags, Bytes(bytes))
            }
            val nel = Nel(events.head, events.tail.toList) // TODO is it optimal convert to list ?
            val result = journals.append(key, nel, timestamp)
            result.map(_ => Nil)
          }
          async.flatten.future
        }
      }

      def delete(persistenceId: PersistenceId, to: SeqNr) = {
        val timestamp = Instant.now()
        val key = toKey(persistenceId)
        journals.delete(key, to, timestamp).future
      }

      def replay(persistenceId: PersistenceId, from: SeqNr, to: SeqNr, max: Long)
        (callback: PersistentRepr => Unit): Future[Unit] = {

        val fold: Fold[Long, Event] = (count, event) => {
          if (event.seqNr <= to && count < max) {
            val persistentEvent = PersistentEventSerializer.fromBinary(event.payload.value)
            val serializedMsg = SerializedMsg(
              persistentEvent.identifier,
              persistentEvent.manifest,
              persistentEvent.payload)
            val payload = serialisation.fromMsg(serializedMsg).get
            val seqNr = persistentEvent.seqNr
            val persistentRepr = PersistentRepr(
              payload = payload,
              sequenceNr = seqNr,
              persistenceId = persistenceId,
              manifest = persistentEvent.persistentManifest,
              writerUuid = persistentEvent.writerUuid)
            callback(persistentRepr)
            val countNew = count + 1
            countNew switch countNew != max
          } else {
            count.stop
          }
        }
        val key = toKey(persistenceId)
        val async = journals.foldWhile(key, from, 0l)(fold)
        async.unit.future
      }

      def highestSeqNr(persistenceId: PersistenceId, from: SeqNr) = {
        val key = toKey(persistenceId)
        journals.lastSeqNr(key, from).future
      }
    }
  }
}

object PayloadAndTags {
  def apply(payload: Any): (Any, Set[String]) = payload match {
    case Tagged(payload, tags) => (payload, tags)
    case _                     => (payload, Set.empty)
  }
}

