package akka.persistence.kafka.journal

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.persistence._
import akka.persistence.journal.JournalSpec
import akka.testkit.DefaultTimeout
import com.evolutiongaming.kafka.journal.Aliases.SeqNr
import com.evolutiongaming.nel.Nel
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers

import scala.concurrent.{Await, Promise}

class ConsistencySpec extends PluginSpec(ConfigFactory.load("consistency.conf")) with KafkaPluginSpec with DefaultTimeout with Matchers {

  type State = Vector[String]

  implicit lazy val system: ActorSystem = ActorSystem("ConsistencySpec", config.withFallback(JournalSpec.config))

  "A KafkaJournal" should {

    "replay events in the same order" in {
      val (refPersist, _) = createRef()
      val events = (1 to 100).toVector map { _.toString }
      for {
        group <- events.grouped(10)
      } {
        val events = Nel.unsafe(group)
        refPersist.persist(events)
      }

      stop(refPersist)

      val (_, state) = createRef()
      state shouldEqual events
    }

    "replay events in the same order when half is deleted" in {
      val (refPersist, _) = createRef()
      val events = (1 to 100).toVector map { _.toString }
      for {
        group <- events.grouped(10)
      } {
        val events = Nel.unsafe(group)
        refPersist.persist(events)
      }

      refPersist.delete(50)
      stop(refPersist)

      val (_, state) = createRef()
      state shouldEqual events.drop(50)
    }
  }


  def createRef() = {
    val promise = Promise[State]()
    val props = Props(actor(pid, promise))
    val ref = system.actorOf(props)
    val future = promise.future
    val state = Await.result(future, timeout.duration)
    (ref, state)
  }

  def actor(id: String, recovered: Promise[State]) = new PersistentActor {
    var state = Vector.empty[String]

    def persistenceId: String = id

    def receiveRecover = {
      case event: String     => state = state :+ event
      case RecoveryCompleted => recovered.success(state)
    }

    def receiveCommand: Receive = {
      case Stop                     => context.stop(self)
      case Delete(seqNr)            => deleteMessages(seqNr)
      case x: DeleteMessagesSuccess => testActor.tell(x, self)
      case x: DeleteMessagesFailure => testActor.tell(x, self)
      case Cmd(events)              =>
        val sender = this.sender()
        val last = events.last
        persistAll(events.toList) { event =>
          state = state :+ event
          if (event == last) sender.tell(event, self)
        }
    }

    override def onRecoveryFailure(cause: Throwable, event: Option[Any]): Unit = {
      recovered.failure(cause)
      super.onRecoveryFailure(cause, event)
    }
  }

  def stop(ref: ActorRef) = {
    watch(ref)
    ref.tell(Stop, testActor)
    expectTerminated(ref)
  }

  case class Cmd(events: Nel[String])
  case class Delete(seqNr: SeqNr)
  case object Stop

  implicit class RefOps(self: ActorRef) {

    def persist(events: Nel[String]): Unit = {
      val cmd = Cmd(events)
      self.tell(cmd, testActor)
      expectMsg(events.last)
    }

    def delete(seqNr: SeqNr): Unit = {
      val delete = Delete(seqNr)
      self.tell(delete, testActor)
      expectMsg(DeleteMessagesSuccess(seqNr))
    }
  }
}
