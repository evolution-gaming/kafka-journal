package com.evolutiongaming.kafka.journal.replicator

import cats.Id
import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem, NonEmptySet => Nes}
import cats.effect.{ExitCase, Resource, Timer}
import cats.implicits._
import com.evolutiongaming.catshelper.TimerHelper._
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.catshelper.DataHelper._
import com.evolutiongaming.retry.{OnError, Retry, Strategy}
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.{RebalanceListener, WithSize}
import com.evolutiongaming.sstream.Stream
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

class ConsumeTopicTest extends AnyFunSuite with Matchers {
  import ConsumeTopicTest._

  test("happy path") {
    val state = State(commands = List(
      Command.AssignPartitions(partitions),
      Command.ProduceRecords(Map.empty),
      Command.ProduceRecords(Map.empty),
      Command.ProduceRecords(recordsOf(recordOf(partition = 0, offset = 0)).toSortedMap)))

    val (result, _) = subscriptionFlow.run(state)

    result shouldEqual State(
      actions = List(
        Action.ReleaseConsumer,
        Action.ReleaseTopicFlow(topic),
        Action.Commit(Nem.of((Partition.min, Offset.min))),
        Action.Poll(recordsOf(recordOf(partition = 0, offset = 0))),
        Action.AssignPartitions(partitions),
        Action.Subscribe()(RebalanceListener.empty),
        Action.AcquireTopicFlow(topic),
        Action.AcquireConsumer))
  }


  test("retry") {
    val state = State(commands = List(
      Command.AssignPartitions(partitions),
      Command.ProduceRecords(Map.empty),
      Command.Fail(Error),
      Command.AssignPartitions(partitions),
      Command.ProduceRecords(Map.empty),
      Command.ProduceRecords(recordsOf(recordOf(partition = 0, offset = 0)).toSortedMap)))

    val (result, _) = subscriptionFlow.run(state)

    result shouldEqual State(
      actions = List(
        Action.ReleaseConsumer,
        Action.ReleaseTopicFlow(topic),
        Action.Commit(Nem.of((Partition.min, Offset.min))),
        Action.Poll(recordsOf(recordOf(partition = 0, offset = 0))),
        Action.AssignPartitions(partitions),
        Action.Subscribe()(RebalanceListener.empty),
        Action.AcquireTopicFlow(topic),
        Action.AcquireConsumer,
        Action.RetryOnError(Error, OnError.Decision.retry(1.millis)),
        Action.ReleaseConsumer,
        Action.ReleaseTopicFlow(topic),
        Action.AssignPartitions(partitions),
        Action.Subscribe()(RebalanceListener.empty),
        Action.AcquireTopicFlow(topic),
        Action.AcquireConsumer))
  }


  test("rebalance") {
    val state = State(commands = List(
      Command.AssignPartitions(Nes.of(Partition.unsafe(1))),
      Command.ProduceRecords(Map.empty),
      Command.AssignPartitions(Nes.of(Partition.unsafe(2))),
      Command.RevokePartitions(Nes.of(Partition.unsafe(1), Partition.unsafe(2))),
      Command.ProduceRecords(recordsOf(recordOf(partition = 0, offset = 0)).toSortedMap)))

    val (result, _) = subscriptionFlow.run(state)

    result shouldEqual State(
      actions = List(
        Action.ReleaseConsumer,
        Action.ReleaseTopicFlow(topic),
        Action.Commit(Nem.of((Partition.min, Offset.min))),
        Action.Poll(recordsOf(recordOf(partition = 0, offset = 0))),
        Action.RevokePartitions(Nes.of(Partition.unsafe(1), Partition.unsafe(2))),
        Action.AssignPartitions(Nes.of(Partition.unsafe(2))),
        Action.AssignPartitions(Nes.of(Partition.unsafe(1))),
        Action.Subscribe()(RebalanceListener.empty),
        Action.AcquireTopicFlow(topic),
        Action.AcquireConsumer))
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


  type StateT[A] = cats.data.StateT[Id, State, Try[A]]

  object StateT {

    def apply[A](f: State => (State, Try[A])): StateT[A] = {
      cats.data.StateT[Id, State, Try[A]](f)
    }

    def pure[A](f: State => (State, A)): StateT[A] = {
      apply { state =>
        val (state1, a) = f(state)
        (state1, a.pure[Try])
      }
    }

    def unit(f: State => State): StateT[Unit] = pure { state => (f(state), ()) }
  }


  implicit val bracketStateT: BracketThrowable[StateT] = new BracketThrowable[StateT] {

    def bracketCase[A, B](
      acquire: StateT[A])(
      use: A => StateT[B])(
      release: (A, ExitCase[Throwable]) => StateT[Unit]
    ) = {

      StateT { s =>
        val (s1, a) = acquire.run(s)
        a.fold(
          a => (s1, a.raiseError[Try, B]),
          a => {
            val (s2, b) = use(a).run(s1)
            val exitCase = b.fold(
              b => ExitCase.error(b),
              _ => ExitCase.complete)
            val (s3, _) = release(a, exitCase).run(s2)
            (s3, b)
          })
      }
    }

    def raiseError[A](a: Throwable) = {
      StateT { s => (s, a.raiseError[Try, A]) }
    }

    def handleErrorWith[A](fa: StateT[A])(f: Throwable => StateT[A]) = {
      StateT { s =>
        val (s1, a) = fa.run(s)
        a.fold(
          a => f(a).run(s1),
          a => (s1, a.pure[Try]))
      }
    }

    def flatMap[A, B](fa: StateT[A])(f: A => StateT[B]) = {
      StateT { s =>
        val (s1, a) = fa.run(s)
        a.fold(a => (s1, a.raiseError[Try, B]), a => f(a).run(s1))
      }
    }

    def tailRecM[A, B](a: A)(f: A => StateT[Either[A, B]]) = {
      @tailrec
      def apply(s: State, a: A): (State, Try[B]) = {
        val (s1, b) = f(a).run(s)
        b match {
          case Success(Right(b)) => (s1, b.pure[Try])
          case Success(Left(b))  => apply(s1, b)
          case Failure(b)        => (s1, b.raiseError[Try, B])
        }
      }

      StateT { s => apply(s, a) }
    }

    def pure[A](a: A) = StateT.pure { s => (s, a) }
  }


  val topicFlowOf: TopicFlowOf[StateT] = {
    topic: Topic => {
      val result = StateT.pure { state =>
        val state1 = state + Action.AcquireTopicFlow(topic)
        val release = StateT.unit { _ + Action.ReleaseTopicFlow(topic) }
        val topicFlow: TopicFlow[StateT] = new TopicFlow[StateT] {

          def assign(partitions: Nes[Partition]) = {
            StateT.unit { _ + Action.AssignPartitions(partitions) }
          }

          def apply(records: Nem[Partition, Nel[ConsRecord]]) = {
            StateT.pure { state =>
              val state1 = state + Action.Poll(records)
              (state1, Map((Partition.min, Offset.min)))
            }
          }

          def revoke(partitions: Nes[Partition]) = {
            StateT.unit { _ + Action.RevokePartitions(partitions) }
          }

          def lose(partitions: Nes[Partition]) = {
            StateT.unit { _ + Action.LosePartitions(partitions) }
          }
        }
        (state1, (topicFlow, release))
      }
      Resource(result)
    }
  }


  val consumer: Resource[StateT, TopicConsumer[StateT]] = {

    val consumer: TopicConsumer[StateT] = new TopicConsumer[StateT] {

      def subscribe(listener: RebalanceListener[StateT]) = {
        StateT.unit { _ + Action.Subscribe()(listener) }
      }

      val poll = {

        def apply(state: State, command: Command) = {
          command match {
            case Command.AssignPartitions(partitions) =>
              state
                .actions
                .collectFirst { case action: Action.Subscribe =>
                  val topicPartitions = partitions.map { partition => TopicPartition(topic, partition) }
                  action
                    .listener
                    .onPartitionsAssigned(topicPartitions)
                    .run(state)
                    .map { case (s, _) => s }
                }
                .getOrElse(state)
                .map { state => (state, Map.empty[Partition, Nel[ConsRecord]].some.pure[Try]) }

            case Command.ProduceRecords(records) =>
              (state, records.some.pure[Try])

            case Command.RevokePartitions(partitions) =>
              state
                .actions
                .collectFirst { case action: Action.Subscribe =>
                  val topicPartitions = partitions.map { partition => TopicPartition(topic, partition) }
                  action
                    .listener
                    .onPartitionsRevoked(topicPartitions)
                    .run(state)
                    .map { case (s, _) => s }
                }
                .getOrElse(state)
                .map { s => (s, Map.empty[Partition, Nel[ConsRecord]].some.pure[Try]) }

            case Command.Fail(error) =>
              (state, error.raiseError[Try, Option[Map[Partition, Nel[ConsRecord]]]])
          }
        }

        val stateT = StateT { state =>
          state.commands match {
            case Nil                 => (state, none[Map[Partition, Nel[ConsRecord]]].pure[Try])
            case command :: commands => apply(state.copy(commands = commands), command)
          }
        }

        Stream.whileSome(stateT)
      }

      val commit = offsets => StateT.unit { _ + Action.Commit(offsets) }
    }

    val result = StateT.pure { state =>
      val state1 = state + Action.AcquireConsumer
      val release = StateT.unit { _ + Action.ReleaseConsumer }
      (state1, (consumer, release))
    }
    Resource(result)
  }


  val retry: Retry[StateT] = {
    val strategy = Strategy.const(1.millis)
    val onError = new OnError[StateT, Throwable] {
      def apply(error: Throwable, status: Retry.Status, decision: OnError.Decision) = {
        StateT.unit { _ + Action.RetryOnError(error, decision) }
      }
    }
    implicit val timer = Timer.empty[StateT]
    Retry(strategy, onError)
  }


  def recordOf(
    partition: Int,
    offset: Long,
  ): ConsRecord = {
    ConsRecord(
      topicPartition = TopicPartition(
        topic = topic,
        partition = Partition.unsafe(partition)),
      offset = Offset.unsafe(offset),
      timestampAndType = none,
      key = WithSize(key).some,
      value = none,
      headers = List.empty)
  }

  def recordsOf(record: ConsRecord, records: ConsRecord*): Nem[Partition, Nel[ConsRecord]] = {
    Nel(record, records.toList)
      .groupBy { _.topicPartition.partition }
      .toNem
      .get
  }


  val subscriptionFlow: StateT[Unit] = ConsumeTopic(
    topic,
    consumer,
    topicFlowOf,
    Log.empty[StateT],
    retry)


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
    final case class AssignPartitions(partitions: Nes[Partition]) extends Action
    final case class RevokePartitions(partitions: Nes[Partition]) extends Action
    final case class LosePartitions(partitions: Nes[Partition]) extends Action
    final case class Subscribe()(val listener: RebalanceListener[StateT]) extends Action
    final case class Poll(records: Nem[Partition, Nel[ConsRecord]]) extends Action
    final case class Commit(offsets: Nem[Partition, Offset]) extends Action
    final case class RetryOnError(error: Throwable, decision: OnError.Decision) extends Action
  }

  case object Error extends RuntimeException with NoStackTrace
}
