// this class was derived from akka.persistence.journal.JournalPerfSpec
package akka.persistence.snapshot

import akka.actor.{ActorLogging, ActorRef, Props}
import akka.annotation.InternalApi
import akka.persistence._
import akka.persistence.snapshot.SnapshotStorePerfSpec._
import akka.serialization.SerializerWithStringManifest
import akka.testkit.TestProbe
import com.typesafe.config.{Config, ConfigFactory}

import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.concurrent.duration._

object SnapshotStorePerfSpec {
  class BenchActor(
    override val persistenceId: String,
    replyTo: ActorRef,
    replyAfter: Int,
    snapshotPerEvents: Int
  ) extends PersistentActor
      with ActorLogging {

    var counter = 0

    var snapshotSavingStartedAt: Map[Long, Long] = Map.empty
    var snapshotSavingFinishedAt: Map[Long, Long] = Map.empty

    // we do not want incoming events to affect measurement of snapshot saving
    // so we will be stashing them when snaphotting is happening
    var savingSnapshot = false

    var loadingSnapshot = true

    override def receiveCommand: Receive = {
      case e: Event =>
        if (savingSnapshot) {
          stash()
        } else {
          persist(e) { d =>
            counter += 1
            require(d.payload == counter, s"Expected to receive [$counter] yet got: [${d.payload}]")
            if (counter % snapshotPerEvents == 0) {
              savingSnapshot = true
              snapshotSavingStartedAt += (this.lastSequenceNr -> System.nanoTime())
              saveSnapshot(Snapshot(counter))
            }
          }
        }

      case s: SaveSnapshotSuccess =>
        savingSnapshot = false
        unstashAll()
        if (snapshotSavingStartedAt.contains(s.metadata.sequenceNr)) {
          snapshotSavingFinishedAt += (s.metadata.sequenceNr -> System.nanoTime())
        } else {
          throw new IllegalArgumentException(
            s"Failed to find a time when snapshot saving started for seqNr: [${s.metadata.sequenceNr}]"
          )
        }
        if (snapshotSavingFinishedAt.size * snapshotPerEvents >= replyAfter) {
          val durations = snapshotSavingStartedAt.map { case (sequenceNr, startedAt) =>
            val finishedAt = snapshotSavingFinishedAt.get(sequenceNr).getOrElse {
              throw new IllegalArgumentException(
                s"Failed to find a time when snapshot saving finished for seqNr: [$sequenceNr]"
              )
            }
            finishedAt - startedAt
          }
          replyTo ! SnapshotsSaved((durations.sum / durations.size).nanos)
        }

      case c: SaveSnapshotFailure =>
        throw new IllegalArgumentException(s"Failed to save a snapshot: [${c.metadata}]", c.cause)

      case DropSnapshots =>
        deleteSnapshots(SnapshotSelectionCriteria.Latest)

      case _: DeleteSnapshotsSuccess =>
        replyTo ! SnapshotsDropped

      case c: DeleteSnapshotFailure =>
        throw new IllegalArgumentException(s"Failed to delete snapshots: [${c.metadata}]", c.cause)

      case ResetCounter =>
        if (savingSnapshot) {
          stash()
        } else {
          snapshotSavingStartedAt = Map.empty
          snapshotSavingFinishedAt = Map.empty
          counter = 0
        }

    }

    override def receiveRecover: Receive = {
      case s @ SnapshotOffer(_, snapshot: Snapshot) =>
        counter = snapshot.counter
        replyTo ! s
      case RecoveryCompleted =>
        loadingSnapshot = false
        replyTo ! RecoveryCompleted
      case _: Event =>
        if (loadingSnapshot) {
          loadingSnapshot = false
          replyTo ! EventsFoundAfterSnapshot
        }
      case other =>
        throw new IllegalArgumentException(s"Got unexpected message on recovery: [$other]")
    }

  }

  sealed trait Command
  case object ResetCounter extends Command
  case class Event(payload: Int) extends Command
  case object DropEvents extends Command
  case object DropSnapshots extends Command

  sealed trait Response
  case class SnapshotsSaved(duration: FiniteDuration) extends Response
  case object EventsDropped extends Command
  case object SnapshotsDropped extends Response
  case object EventsFoundAfterSnapshot extends Response

  case class Snapshot(counter: Int)

  /** INTERNAL API
    */
  @InternalApi private[akka] class EventAndSnapshotSerializer extends SerializerWithStringManifest {
    override def identifier: Int = 1018284148

    override def manifest(o: AnyRef): String =
      o match {
        case _: Event    => "E"
        case _: Snapshot => "S"
        case _ =>
          throw new IllegalArgumentException(
            s"Can't find manifest for object of type ${o.getClass} in [${getClass.getName}]"
          )
      }

    override def toBinary(o: AnyRef): Array[Byte] =
      o match {
        case Event(payload) =>
          s"$payload".getBytes(StandardCharsets.UTF_8)
        case Snapshot(counter) =>
          s"$counter".getBytes(StandardCharsets.UTF_8)
        case _ =>
          throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
      }

    override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
      manifest match {
        case "E" =>
          val str = new String(bytes, StandardCharsets.UTF_8)
          Event(str.toInt)
        case "S" =>
          val str = new String(bytes, StandardCharsets.UTF_8)
          Snapshot(str.toInt)
        case other =>
          throw new IllegalArgumentException(s"Can't recognize manifest $other in [${getClass.getName}]")
      }
    }
  }

  private val eventSerializerConfig = ConfigFactory.parseString(s"""
  akka.actor {
    serializers {
      SnapshotStorePerfSpecSerializer = "${classOf[EventAndSnapshotSerializer].getName}"
    }
    serialization-bindings {
      "${classOf[Event].getName}" = SnapshotStorePerfSpecSerializer
      "${classOf[Snapshot].getName}" = SnapshotStorePerfSpecSerializer
    }
  }
  """)
}

/** This spec measures execution times of the basic operations that an [[akka.persistence.PersistentActor]] provides,
  * using the provided SnapshotStore (plugin).
  *
  * It is *NOT* meant to be a comprehensive benchmark, but rather aims to help plugin developers to easily determine if
  * their plugin's performance is roughly as expected. It also validates the plugin still works under "more snapshots"
  * scenarios.
  *
  * In case your snapshot plugin needs some kind of setup or teardown, override the `beforeAll` or `afterAll` methods
  * (don't forget to call `super` in your overridden methods).
  *
  * For a Java and JUnit consumable version of the TCK please refer to
  * [[akka.persistence.japi.snapshot.JavaSnapshotStorePerfSpec]].
  *
  * @see
  *   [[akka.persistence.snapshot.SnapshotStoreSpec]]
  */
abstract class SnapshotStorePerfSpec(config: Config)
    extends SnapshotStoreSpec(config.withFallback(SnapshotStorePerfSpec.eventSerializerConfig)) {

  private val testProbe = TestProbe()

  def benchActor(replyAfter: Int): ActorRef =
    system.actorOf(
      Props(
        classOf[BenchActor],
        "SnapshotStorePerfSpec-bench",
        testProbe.ref,
        replyAfter,
        snapshotPerEvents
      )
    )

  /** Override in order to customize timeouts used for expectMsg, in order to tune the awaits to your snapshot's perf */
  def awaitDurationMillis: Long = 10.seconds.toMillis

  /** Override in order to customize timeouts used for expectMsg, in order to tune the awaits to your snapshot's perf */
  private def awaitDuration: FiniteDuration = awaitDurationMillis.millis

  /** Number of events sent to the PersistentActor under test for each test iteration */
  def eventsCount: Int = 10 * 1000

  /** How often snapshot should be made */
  def snapshotPerEvents: Int = 10

  /** Number of measurement iterations each test will be run. */
  def measurementIterations: Int = 10

  def snapshotCount: Int = eventsCount / snapshotPerEvents

  "A PersistentActor's performance" must {
    s"measure: saveSnapshot()-ing $snapshotCount snapshots" in {
      val p1 = benchActor(eventsCount)
      testProbe.expectMsgType[RecoveryCompleted](awaitDuration)

      (0 until measurementIterations).foreach { _ =>
        (1 to eventsCount).foreach { i =>
          p1 ! Event(i)
        }
        val response = testProbe.expectMsgType[SnapshotsSaved](awaitDuration)
        val duration = response.duration.toNanos / 1000000.0
        info(s"Average time: ${duration} milliseconds")
        p1 ! ResetCounter
      }
    }
    s"measure: recovering after doing ${snapshotCount * measurementIterations} snapshots" in {
      (0 until measurementIterations).foreach { _ =>
        val startedAt = Instant.now()
        benchActor(snapshotCount)
        testProbe.expectMsgType[SnapshotOffer](awaitDuration)
        val finishedAt = Instant.now()
        val duration = (finishedAt.toEpochMilli - startedAt.toEpochMilli).millis
        info(s"Recovering snapshot took $duration")

        // wait until events recovered
        testProbe.expectMsgType[RecoveryCompleted](awaitDuration)
      }
    }
    s"measure: recovering after deleting all the snapshots" in {
      val p1 = benchActor(eventsCount)
      testProbe.expectMsgType[SnapshotOffer](awaitDuration)
      testProbe.expectMsgType[RecoveryCompleted](awaitDuration)
      p1 ! DropSnapshots
      testProbe.expectMsg(awaitDuration, SnapshotsDropped)

      (0 until measurementIterations).foreach { _ =>
        val startedAt = Instant.now()
        benchActor(snapshotCount)
        testProbe.expectMsg(awaitDuration, EventsFoundAfterSnapshot)
        val finishedAt = Instant.now()
        val duration = (finishedAt.toEpochMilli - startedAt.toEpochMilli).millis
        info(s"Recovering snapshot took $duration")

        // wait until events recovered
        testProbe.expectMsgType[RecoveryCompleted](awaitDuration)
      }
    }
  }

}
