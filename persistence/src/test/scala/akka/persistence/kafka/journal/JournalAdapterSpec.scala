package akka.persistence.kafka.journal

import java.time.Instant
import java.time.temporal.ChronoUnit

import akka.persistence.{AtomicWrite, PersistentRepr}
import cats.Id
import cats.data.StateT
import cats.effect.Clock
import cats.implicits._
import com.evolutiongaming.kafka.journal.{ClockOf, _}
import com.evolutiongaming.kafka.journal.stream.Stream
import com.evolutiongaming.nel.Nel
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.JsValue

class JournalAdapterSpec extends FunSuite with Matchers {
  import JournalAdapterSpec.StateF._
  import JournalAdapterSpec._

  private val toKey = ToKey.Default
  private val key1 = Key(id = "id", topic = "journal")
  private val event = Event(SeqNr.Min)
  private val persistenceId = "id"
  private val persistentRepr = PersistentRepr(None, persistenceId = persistenceId)

  private val eventSerializer = new EventSerializer {

    def toEvent(persistentRepr: PersistentRepr) = event

    def toPersistentRepr(persistenceId: PersistenceId, event: Event) = persistentRepr
  }

  val journalAdapter = JournalAdapter[StateF](StateF.JournalStateF, toKey, eventSerializer)

  test("write") {
    val (data, result) = journalAdapter.write(List(AtomicWrite(List(persistentRepr)))).run(Data.Empty)
    result shouldEqual Nil
    data shouldEqual Data(appends = List(Append(key1, Nel(event), timestamp)))
  }

  test("delete") {
    val (data, _) = journalAdapter.delete(persistenceId, SeqNr.Max).run(Data.Empty)
    data shouldEqual Data(deletes = List(Delete(key1, SeqNr.Max, timestamp)))
  }

  test("lastSeqNr") {
    val (data, result) = journalAdapter.lastSeqNr(persistenceId, SeqNr.Max).run(Data.Empty)
    result shouldEqual None
    data shouldEqual Data(pointers = List(Pointer(key1)))
  }

  test("replay") {
    val range = SeqRange(from = SeqNr.Min, to = SeqNr.Max)
    var prs = List.empty[PersistentRepr]
    val initial = Data(events = List(event))
    val (data, _) = journalAdapter.replay(persistenceId, range, Int.MaxValue)(pr => prs = pr :: prs).run(initial)
    data shouldEqual Data(reads = List(Read(key1, SeqNr.Min)))
    prs shouldEqual List(persistentRepr)
  }
}

object JournalAdapterSpec {

  val timestamp: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS)

  final case class Append(key: Key, events: Nel[Event], timestamp: Instant)

  final case class Read(key: Key, from: SeqNr)

  final case class Delete(key: Key, to: SeqNr, timestamp: Instant)

  final case class Pointer(key: Key)

  final case class Data(
    events: List[Event] = Nil,
    appends: List[Append] = Nil,
    pointers: List[Pointer] = Nil,
    deletes: List[Delete] = Nil,
    reads: List[Read] = Nil)

  object Data {
    val Empty: Data = Data()
  }


  type StateF[A] = StateT[Id, Data, A]

  object StateF {

    implicit val LogStateF: Log[StateF] = Log.empty[StateF]

    implicit val ClockStateF: Clock[StateF] = ClockOf(timestamp.toEpochMilli)

    implicit val JournalStateF: Journal[StateF] = new Journal[StateF] {

      def append(key: Key, events: Nel[Event], metadata: Option[JsValue]) = {
        StateF { s =>
          val s1 = s.copy(appends = Append(key, events, timestamp) :: s.appends)
          (s1, PartitionOffset.Empty)
        }
      }

      def read(key: Key, from: SeqNr) = {
        val stream = StateF { state =>
          val stream = Stream[StateF].apply(state.events)
          val state1 = state.copy(reads = Read(key, from) :: state.reads, events = Nil)
          (state1, stream)
        }
        Stream.lift(stream).flatten
      }
      
      def pointer(key: Key) = {
        StateF { state =>
          val state1 = state.copy(pointers = Pointer(key) :: state.pointers)
          (state1, none[SeqNr])
        }
      }

      def delete(key: Key, to: SeqNr) = {
        StateF { state =>
          val state1 = state.copy(deletes = Delete(key, to, timestamp) :: state.deletes)
          (state1, none[PartitionOffset])
        }
      }
    }

    def apply[A](f: Data => (Data, A)): StateF[A] = StateT[Id, Data, A](f)
  }
}
