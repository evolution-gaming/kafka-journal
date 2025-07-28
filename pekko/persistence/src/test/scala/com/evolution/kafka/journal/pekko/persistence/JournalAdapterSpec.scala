package com.evolution.kafka.journal.pekko.persistence

import cats.data.NonEmptyList as Nel
import cats.effect.Clock
import cats.syntax.all.*
import com.evolution.kafka.journal.ExpireAfter.implicits.*
import com.evolution.kafka.journal.conversions.{KafkaRead, KafkaWrite}
import com.evolution.kafka.journal.eventual.{EventualPayloadAndType, EventualRead, EventualWrite}
import com.evolution.kafka.journal.util.Fail
import com.evolution.kafka.journal.util.StreamHelper.*
import com.evolution.kafka.journal.*
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.catshelper.{FromTry, Log}
import com.evolutiongaming.sstream.Stream
import org.apache.pekko.persistence.{AtomicWrite, PersistentRepr}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.Json

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.*
import scala.util.Try

class JournalAdapterSpec extends AnyFunSuite with Matchers {
  import JournalAdapterSpec.*
  import JournalAdapterSpec.StateT.*
  import TestJsonCodec.instance

  private val eventSerializer = EventSerializer.const[StateT, Payload](event, persistentRepr)
  private val journalReadWrite = JournalReadWrite.of[StateT, Payload]

  private val aws = List(AtomicWrite(List(persistentRepr)), AtomicWrite(List(persistentRepr)))

  private val appendMetadataOf = {
    val metadata = AppendMetadata(recordMetadata, headers)
    AppendMetadataOf.const(metadata.pure[StateT])
  }

  private val journalAdapter =
    JournalAdapter[StateT, Payload](StateT.JournalsStateF, toKey, eventSerializer, journalReadWrite, appendMetadataOf)

  private def appendOf(key: Key, events: Nel[Event[Payload]]) = {
    val payloadAndMetadata = KafkaWrite.summon[Try, Payload].apply(Events(events, recordMetadata.payload)).get
    Append(key, payloadAndMetadata, recordMetadata, headers)
  }

  test("write") {
    val (data, result) = journalAdapter.write(aws).run(State.empty).get
    result shouldEqual Nil
    data shouldEqual State(appends = List(appendOf(key1, Nel.of(event, event))))
  }

  test("delete") {
    val (data, _) = journalAdapter.delete(persistenceId, DeleteTo.max).run(State.empty).get
    data shouldEqual State(deletes = List(Delete(key1, DeleteTo.max, timestamp)))
  }

  test("lastSeqNr") {
    val (data, result) = journalAdapter.lastSeqNr(persistenceId, SeqNr.max).run(State.empty).get
    result shouldEqual None
    data shouldEqual State(pointers = List(Pointer(key1)))
  }

  test("replay") {
    val range = SeqRange(from = SeqNr.min, to = SeqNr.max)
    val initial = State(events = List(eventRecord.map(EventualWrite.summon[Try, Payload].apply(_).get)))
    val f = (a: PersistentRepr) =>
      StateT { s =>
        val s1 = s.copy(replayed = a :: s.replayed)
        (s1, ())
      }
    val (data, _) = journalAdapter.replay(persistenceId, range, Int.MaxValue)(f).run(initial).get
    data shouldEqual State(reads = List(Read(key1, SeqNr.min)), replayed = List(persistentRepr))
  }

  test("withBatching") {
    val grouping = new Batching[StateT] {
      def apply(aws: List[AtomicWrite]): StateT[List[List[AtomicWrite]]] = aws.map(aw => List(aw)).pure[StateT]
    }
    val (data, result) = journalAdapter
      .withBatching(grouping)
      .write(aws)
      .run(State.empty)
      .get
    result shouldEqual Nil
    data shouldEqual State(appends = List(appendOf(key1, Nel.of(event)), appendOf(key1, Nel.of(event))))
  }
}

object JournalAdapterSpec {

  private val timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS)
  private val expireAfter = 1.day.toExpireAfter
  private val toKey = ToKey.default[StateT]
  private val key1 = Key(id = "id", topic = "journal")
  private val event = Event(SeqNr.min, payload = Payload.text("payload").some)
  private val partitionOffset = PartitionOffset.empty
  private val persistenceId = "id"
  private val persistentRepr = PersistentRepr(None, persistenceId = persistenceId)
  private val recordMetadata =
    RecordMetadata(HeaderMetadata(Json.obj(("key", "value")).some), PayloadMetadata(expireAfter.some))
  private val headers = Headers(("key", "value"))
  private val origin = Origin("origin")
  private val version = Version.current
  private val eventRecord =
    EventRecord(event, timestamp, partitionOffset, origin.some, version.some, recordMetadata, headers)

  sealed abstract class Action

  object Action {
    final case class Purge(key: Key) extends Action
  }

  final case class Append(
    key: Key,
    payloadAndType: PayloadAndType,
    metadata: RecordMetadata,
    headers: Headers,
  )

  final case class Read(key: Key, from: SeqNr)

  final case class Delete(key: Key, to: DeleteTo, timestamp: Instant)

  final case class Pointer(key: Key)

  final case class State(
    events: List[EventRecord[EventualPayloadAndType]] = List.empty,
    appends: List[Append] = List.empty,
    pointers: List[Pointer] = List.empty,
    deletes: List[Delete] = List.empty,
    reads: List[Read] = List.empty,
    replayed: List[PersistentRepr] = List.empty,
    actions: List[Action] = List.empty,
  )

  object State {

    val empty: State = State()

    implicit class StateOps(val self: State) extends AnyVal {

      def append(action: Action): State = self.copy(actions = action :: self.actions)
    }
  }

  type StateT[A] = cats.data.StateT[Try, State, A]

  object StateT {

    implicit val fail: Fail[StateT] = Fail.lift[StateT]

    implicit val fromTry: FromTry[StateT] = FromTry.lift[StateT]

    implicit val fromAttempt: FromAttempt[StateT] = FromAttempt.lift[StateT]

    implicit val fromJsResult: FromJsResult[StateT] = FromJsResult.lift[StateT]

    implicit val LogStateF: Log[StateT] = Log.empty[StateT]

    implicit val clockStateF: Clock[StateT] =
      Clock.const(nanos = 0, millis = timestamp.toEpochMilli) // TODO add Instant as argument

    implicit val JournalsStateF: Journals[StateT] = new Journals[StateT] {

      def apply(key: Key): Journal[StateT] = new Journal[StateT] {

        def append[A](
          events: Nel[Event[A]],
          metadata: RecordMetadata,
          headers: Headers,
        )(implicit
          kafkaWrite: KafkaWrite[StateT, A],
        ): StateT[PartitionOffset] =
          for {
            payloadAndType <- kafkaWrite(Events(events, metadata.payload))
            append = Append(key, payloadAndType, metadata, headers)
            offset <- StateT { state =>
              val s1 = state.copy(appends = append :: state.appends)
              (s1, partitionOffset)
            }
          } yield offset

        def read[A](
          from: SeqNr,
        )(implicit
          kafkaRead: KafkaRead[StateT, A],
          eventualRead: EventualRead[StateT, A],
        ): Stream[StateT, EventRecord[A]] = {
          val stream = StateT { state =>
            val events = state.events.traverse(_.traverse(eventualRead.apply))

            val read = Read(key, from)
            val state1 = state.copy(reads = read :: state.reads, events = Nil)

            (state1, events)
          }
            .flatten
            .map { Stream[StateT].apply(_) }

          stream.toStream.flatten
        }

        def pointer: StateT[Option[SeqNr]] = {
          StateT { state =>
            val state1 = state.copy(pointers = Pointer(key) :: state.pointers)
            (state1, none[SeqNr])
          }
        }

        def delete(to: DeleteTo): StateT[Option[PartitionOffset]] = {
          StateT { state =>
            val delete = Delete(key, to, timestamp)
            val state1 = state.copy(deletes = delete :: state.deletes)
            (state1, none[PartitionOffset])
          }
        }

        def purge: StateT[Option[PartitionOffset]] = {
          StateT { state =>
            val state1 = state.append(Action.Purge(key))
            (state1, none[PartitionOffset])
          }
        }
      }
    }

    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[Try, State, A] { a => f(a).pure[Try] }
  }
}
