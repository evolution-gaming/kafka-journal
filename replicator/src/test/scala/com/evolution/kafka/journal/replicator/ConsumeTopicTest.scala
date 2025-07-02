package com.evolution.kafka.journal.replicator

import cats.data.{NonEmptyList as Nel, NonEmptyMap as Nem, NonEmptySet as Nes}
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.catshelper.{Log, ToTry}
import com.evolution.kafka.journal.ConsRecord
import com.evolution.kafka.journal.util.TestTemporal.temporalTry
import com.evolutiongaming.retry.{OnError, Retry, Strategy}
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.RebalanceCallback.syntax.*
import com.evolutiongaming.skafka.consumer.{RebalanceCallback, RebalanceListener1, WithSize}
import com.evolutiongaming.sstream.Stream
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

class ConsumeTopicTest extends AsyncFunSuite with Matchers {

  import ConsumeTopicTest.*

  test("happy path") {
    val state = State(commands =
      List(
        Command.AssignPartitions(partitions),
        Command.ProduceRecords(Map.empty),
        Command.ProduceRecords(Map.empty),
        Command.ProduceRecords(recordsOf(recordOf(partition = 0, offset = 0)).toSortedMap),
      ),
    )

    subscriptionFlow.run(state).unsafeToFuture().map {
      case (result, _) =>
        result shouldEqual State(
          actions = List(
            Action.ReleaseConsumer,
            Action.ReleaseTopicFlow(topic),
            Action.Commit(Nem.of((Partition.min, Offset.min))),
            Action.Poll(recordsOf(recordOf(partition = 0, offset = 0))),
            Action.CommitAssignPartitions(partitions),
            Action.FlowAssignPartitions(partitions),
            Action.Subscribe()(RebalanceListener1.empty[StateT]),
            Action.AcquireTopicFlow(topic),
            Action.AcquireConsumer,
          ),
        )
    }
  }

  test("retry") {
    val state = State(commands =
      List(
        Command.AssignPartitions(partitions),
        Command.ProduceRecords(Map.empty),
        Command.Fail(Error),
        Command.AssignPartitions(partitions),
        Command.ProduceRecords(Map.empty),
        Command.ProduceRecords(recordsOf(recordOf(partition = 0, offset = 0)).toSortedMap),
      ),
    )

    subscriptionFlow.run(state).unsafeToFuture().map {
      case (result, _) =>
        result shouldEqual State(
          actions = List(
            Action.ReleaseConsumer,
            Action.ReleaseTopicFlow(topic),
            Action.Commit(Nem.of((Partition.min, Offset.min))),
            Action.Poll(recordsOf(recordOf(partition = 0, offset = 0))),
            Action.CommitAssignPartitions(partitions),
            Action.FlowAssignPartitions(partitions),
            Action.Subscribe()(RebalanceListener1.empty),
            Action.AcquireTopicFlow(topic),
            Action.AcquireConsumer,
            Action.RetryOnError(Error, OnError.Decision.retry(1.millis)),
            Action.ReleaseConsumer,
            Action.ReleaseTopicFlow(topic),
            Action.CommitAssignPartitions(partitions),
            Action.FlowAssignPartitions(partitions),
            Action.Subscribe()(RebalanceListener1.empty),
            Action.AcquireTopicFlow(topic),
            Action.AcquireConsumer,
          ),
        )
    }
  }

  test("rebalance") {
    val state = State(commands =
      List(
        Command.AssignPartitions(Nes.of(Partition.unsafe(1))),
        Command.ProduceRecords(Map.empty),
        Command.AssignPartitions(Nes.of(Partition.unsafe(2))),
        Command.RevokePartitions(Nes.of(Partition.unsafe(1), Partition.unsafe(2))),
        Command.ProduceRecords(recordsOf(recordOf(partition = 0, offset = 0)).toSortedMap),
      ),
    )

    subscriptionFlow.run(state).unsafeToFuture().map {
      case (result, _) =>
        result shouldEqual State(
          actions = List(
            Action.ReleaseConsumer,
            Action.ReleaseTopicFlow(topic),
            Action.Commit(Nem.of((Partition.min, Offset.min))),
            Action.Poll(recordsOf(recordOf(partition = 0, offset = 0))),
            Action.CommitRevokePartitions(Nes.of(Partition.unsafe(1), Partition.unsafe(2))),
            Action.FlowRevokePartitions(Nes.of(Partition.unsafe(1), Partition.unsafe(2))),
            Action.CommitAssignPartitions(Nes.of(Partition.unsafe(2))),
            Action.FlowAssignPartitions(Nes.of(Partition.unsafe(2))),
            Action.CommitAssignPartitions(Nes.of(Partition.unsafe(1))),
            Action.FlowAssignPartitions(Nes.of(Partition.unsafe(1))),
            Action.Subscribe()(RebalanceListener1.empty[StateT]),
            Action.AcquireTopicFlow(topic),
            Action.AcquireConsumer,
          ),
        )
    }
  }
}

object ConsumeTopicTest {

  val topic: Topic = "topic"

  val key: String = "key"

  val partitions: Nes[Partition] = Nes.of(Partition.min)

  final case class State(
    commands: List[Command] = List.empty,
    actions: List[Action] = List.empty,
  ) {

    def +(action: Action): State = copy(actions = action :: actions)
  }

  type StateT[A] = cats.data.StateT[IO, State, Try[A]]

  object StateT {

    def of[A](f: State => IO[(State, Try[A])]): StateT[A] = {
      cats.data.StateT[IO, State, Try[A]](s => f(s))
    }

    def apply[A](f: State => (State, Try[A])): StateT[A] = {
      cats.data.StateT[IO, State, Try[A]](s => IO.delay(f(s)))
    }

    def pure[A](f: State => (State, A)): StateT[A] = {
      apply { state =>
        val (state1, a) = f(state)
        (state1, a.pure[Try])
      }
    }

    def unit(f: State => State): StateT[Unit] = pure { state => (f(state), ()) }
  }

  implicit class RebalanceCallbackOps[A](val callback: RebalanceCallback[StateT, A]) extends AnyVal {

    def runAsIO(state: State): IO[(State, A)] = {

      IO.delay {
        var result: State = state

        implicit val toTryStateT: ToTry[StateT] = new ToTry[StateT] {

          val ioToTry = ToTry.ioToTry

          def apply[B](stateT: StateT[B]): Try[B] = {
            val io = stateT.run(result)
            val tr = ioToTry(io)
            tr match {
              case Success((s, tryB)) => result = s; tryB
              case Failure(exception) => Failure(exception)
            }
          }
        }

        callback.run(EmptyRebalanceConsumer) match {
          case Failure(e) => throw e
          case Success(a) => result -> a
        }
      }

    }

  }

  val topicFlowOf: TopicFlowOf[StateT] = { (topic: Topic) =>
    {
      val result = StateT.pure { state =>
        val state1 = state + Action.AcquireTopicFlow(topic)
        val release = StateT.unit {
          _ + Action.ReleaseTopicFlow(topic)
        }
        val topicFlow: TopicFlow[StateT] = new TopicFlow[StateT] {

          def assign(partitions: Nes[Partition]) = {
            StateT.unit {
              _ + Action.FlowAssignPartitions(partitions)
            }
          }

          def apply(records: Nem[Partition, Nel[ConsRecord]]) = {
            StateT.pure { state =>
              val state1 = state + Action.Poll(records)
              (state1, Map((Partition.min, Offset.min)))
            }
          }

          def revoke(partitions: Nes[Partition]) = {
            StateT.unit {
              _ + Action.FlowRevokePartitions(partitions)
            }
          }

          def lose(partitions: Nes[Partition]) = {
            StateT.unit {
              _ + Action.FlowLosePartitions(partitions)
            }
          }
        }
        (state1, (topicFlow, release))
      }
      Resource(result)
    }
  }

  val consumer: Resource[StateT, TopicConsumer[StateT]] = {

    val consumer: TopicConsumer[StateT] = new TopicConsumer[StateT] {

      def subscribe(listener: RebalanceListener1[StateT]) = {
        StateT.unit {
          _ + Action.Subscribe()(listener)
        }
      }

      val poll = {

        def apply(state: State, command: Command) = {
          command match {
            case Command.AssignPartitions(partitions) =>
              state
                .actions
                .collectFirst {
                  case action: Action.Subscribe =>
                    val topicPartitions = partitions.map { partition => TopicPartition(topic, partition) }
                    action
                      .listener
                      .onPartitionsAssigned(topicPartitions)
                      .runAsIO(state)
                      .map { case (s, _) => s }
                }
                .getOrElse(state.pure[IO])
                .map { state => (state, Map.empty[Partition, Nel[ConsRecord]].some.pure[Try]) }

            case Command.ProduceRecords(records) =>
              IO.pure((state, records.some.pure[Try]))

            case Command.RevokePartitions(partitions) =>
              state
                .actions
                .collectFirst {
                  case action: Action.Subscribe =>
                    val topicPartitions = partitions.map { partition => TopicPartition(topic, partition) }
                    action
                      .listener
                      .onPartitionsRevoked(topicPartitions)
                      .runAsIO(state)
                      .map { case (s, _) => s }
                }
                .getOrElse(state.pure[IO])
                .map { s => (s, Map.empty[Partition, Nel[ConsRecord]].some.pure[Try]) }

            case Command.Fail(error) =>
              IO.delay((state, error.raiseError[Try, Option[Map[Partition, Nel[ConsRecord]]]]))
          }
        }

        val stateT = StateT.of { state =>
          state.commands match {
            case Nil => (state, none[Map[Partition, Nel[ConsRecord]]].pure[Try]).pure[IO]
            case command :: commands => apply(state.copy(commands = commands), command)
          }
        }

        Stream.whileSome(stateT)
      }

      val commit: TopicCommit[StateT] = {
        new TopicCommit[StateT] {

          override def apply(offsets: Nem[Partition, Offset]): StateT[Unit] = {
            StateT.unit {
              _ + Action.Commit(offsets)
            }
          }

          override def onPartitionsAssigned(partitions: Nes[Partition]): RebalanceCallback[StateT, Unit] = {
            StateT.unit {
              _ + Action.CommitAssignPartitions(partitions)
            }.lift
          }

          override def onPartitionsRevoked(partitions: Nes[Partition]): RebalanceCallback[StateT, Unit] = {
            StateT.unit {
              _ + Action.CommitRevokePartitions(partitions)
            }.lift
          }

          override def onPartitionsLost(partitions: Nes[Partition]): RebalanceCallback[StateT, Unit] = {
            StateT.unit {
              _ + Action.CommitLosePartitions(partitions)
            }.lift
          }
        }
      }
    }

    val result = StateT.pure { state =>
      val state1 = state + Action.AcquireConsumer
      val release = StateT.unit {
        _ + Action.ReleaseConsumer
      }
      (state1, (consumer, release))
    }
    Resource(result)
  }

  val retry: Retry[StateT] = {
    val strategy = Strategy.const(1.millis)
    val onError = new OnError[StateT, Throwable] {
      def apply(error: Throwable, status: Retry.Status, decision: OnError.Decision) = {
        StateT.unit {
          _ + Action.RetryOnError(error, decision)
        }
      }
    }
    Retry(strategy, onError)
  }

  def recordOf(
    partition: Int,
    offset: Long,
  ): ConsRecord = {
    ConsRecord(
      topicPartition = TopicPartition(topic = topic, partition = Partition.unsafe(partition)),
      offset = Offset.unsafe(offset),
      timestampAndType = none,
      key = WithSize(key).some,
      value = none,
      headers = List.empty,
    )
  }

  def recordsOf(record: ConsRecord, records: ConsRecord*): Nem[Partition, Nel[ConsRecord]] = {
    Nel(record, records.toList)
      .groupBy {
        _.topicPartition.partition
      }
      .toNem()
      .get
  }

  val subscriptionFlow: StateT[Unit] = ConsumeTopic(topic, consumer, topicFlowOf, Log.empty[StateT], retry)

  sealed abstract class Command

  object Command {
    final case class ProduceRecords(records: Map[Partition, Nel[ConsRecord]]) extends Command

    final case class AssignPartitions(partitions: Nes[Partition]) extends Command

    final case class RevokePartitions(partitions: Nes[Partition]) extends Command

    final case class Fail(error: Throwable) extends Command
  }

  sealed abstract class Action

  object Action {
    final case class AcquireTopicFlow(topic: Topic) extends Action

    final case class ReleaseTopicFlow(topic: Topic) extends Action

    case object AcquireConsumer extends Action

    case object ReleaseConsumer extends Action

    final case class FlowAssignPartitions(partitions: Nes[Partition]) extends Action

    final case class FlowRevokePartitions(partitions: Nes[Partition]) extends Action

    final case class FlowLosePartitions(partitions: Nes[Partition]) extends Action

    final case class CommitAssignPartitions(partitions: Nes[Partition]) extends Action

    final case class CommitRevokePartitions(partitions: Nes[Partition]) extends Action

    final case class CommitLosePartitions(partitions: Nes[Partition]) extends Action

    final case class Subscribe()(val listener: RebalanceListener1[StateT]) extends Action

    final case class Poll(records: Nem[Partition, Nel[ConsRecord]]) extends Action

    final case class Commit(offsets: Nem[Partition, Offset]) extends Action

    final case class RetryOnError(error: Throwable, decision: OnError.Decision) extends Action
  }

  case object Error extends RuntimeException with NoStackTrace
}
