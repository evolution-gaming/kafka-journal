package com.evolutiongaming.kafka.journal.replicator

import cats.Id
import cats.data.{NonEmptyList => Nel}
import cats.effect.{ExitCase, Resource, Timer}
import cats.implicits._
import com.evolutiongaming.catshelper.TimerHelper._
import com.evolutiongaming.catshelper.BracketThrowable
import com.evolutiongaming.kafka.journal.replicator.SubscriptionFlow.Consumer
import com.evolutiongaming.retry.{OnError, Retry, Strategy}
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords, RebalanceListener, WithSize}
import com.evolutiongaming.sstream.Stream
import org.scalatest.{FunSuite, Matchers}
import scodec.bits.ByteVector

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

class SubscriptionFlowTest extends FunSuite with Matchers {
  import SubscriptionFlowTest._

  test("happy path") {
    val state = State(commands = List(
      Command.AssignPartitions(partitions),
      Command.ProduceRecords(ConsumerRecords.empty),
      Command.ProduceRecords(ConsumerRecords.empty),
      Command.ProduceRecords(consumerRecords(consumerRecord(partition = 0, offset = 0)))))

    val (result, _) = subscriptionFlow.take(1).toList.run(state)

    result shouldEqual State(
      actions = List(
        Action.ReleaseConsumer,
        Action.ReleaseTopicFlow(topic),
        Action.Poll(consumerRecords(consumerRecord(partition = 0, offset = 0))),
        Action.AssignPartitions(partitions),
        Action.Subscribe(topic)(RebalanceListener.empty),
        Action.AcquireTopicFlow(topic),
        Action.AcquireConsumer))
  }


  test("retry") {
    val state = State(commands = List(
      Command.AssignPartitions(partitions),
      Command.ProduceRecords(ConsumerRecords.empty),
      Command.Fail(Error),
      Command.AssignPartitions(partitions),
      Command.ProduceRecords(ConsumerRecords.empty),
      Command.ProduceRecords(consumerRecords(consumerRecord(partition = 0, offset = 0)))))

    val (result, _) = subscriptionFlow.take(1).toList.run(state)

    result shouldEqual State(
      actions = List(
        Action.ReleaseConsumer,
        Action.ReleaseTopicFlow(topic),
        Action.Poll(consumerRecords(consumerRecord(partition = 0, offset = 0))),
        Action.AssignPartitions(partitions),
        Action.Subscribe(topic)(RebalanceListener.empty),
        Action.AcquireTopicFlow(topic),
        Action.AcquireConsumer,
        Action.RetryOnError(Error, OnError.Decision.retry(1.millis)),
        Action.ReleaseConsumer,
        Action.ReleaseTopicFlow(topic),
        Action.AssignPartitions(partitions),
        Action.Subscribe(topic)(RebalanceListener.empty),
        Action.AcquireTopicFlow(topic),
        Action.AcquireConsumer))
  }


  test("rebalance") {
    val state = State(commands = List(
      Command.AssignPartitions(Nel.of(1)),
      Command.ProduceRecords(ConsumerRecords.empty),
      Command.AssignPartitions(Nel.of(2)),
      Command.RevokePartitions(Nel.of(1, 2)),
      Command.ProduceRecords(consumerRecords(consumerRecord(partition = 0, offset = 0)))))

    val (result, _) = subscriptionFlow.take(1).toList.run(state)

    result shouldEqual State(
      actions = List(
        Action.ReleaseConsumer,
        Action.ReleaseTopicFlow(topic),
        Action.Poll(consumerRecords(consumerRecord(partition = 0, offset = 0))),
        Action.RevokePartitions(Nel.of(1, 2)),
        Action.AssignPartitions(Nel.of(2)),
        Action.AssignPartitions(Nel.of(1)),
        Action.Subscribe(topic)(RebalanceListener.empty),
        Action.AcquireTopicFlow(topic),
        Action.AcquireConsumer))
  }
}

object SubscriptionFlowTest {

  val topic: Topic = "topic"

  val key: String = "key"

  val partitions: Nel[Partition] = Nel.of(1)


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
    (topic: Topic, _: Consumer[StateT]/*TODO remove*/) => {
      val result = StateT.pure { state =>
        val state1 = state + Action.AcquireTopicFlow(topic)
        val release = StateT.unit { _ + Action.ReleaseTopicFlow(topic) }
        val topicFlow: TopicFlow[StateT] = new TopicFlow[StateT] {

          def assign(partitions: Nel[Partition]) = {
            StateT.unit { _ + Action.AssignPartitions(partitions) }
          }

          def apply(consumerRecords: ConsumerRecords[String, ByteVector]) = {
            StateT.unit { _ + Action.Poll(consumerRecords) }
          }

          def revoke(partitions: Nel[Partition]) = {
            StateT.unit { _ + Action.RevokePartitions(partitions) }
          }
        }
        (state1, (topicFlow, release))
      }
      Resource(result)
    }
  }


  val consumer: Resource[StateT, Consumer[StateT]] = {

    val consumer: Consumer[StateT] = new Consumer[StateT] {

      def subscribe(topic: Topic, listener: RebalanceListener[StateT]) = {
        StateT.unit { _ + Action.Subscribe(topic)(listener) }
      }

      def poll(timeout: FiniteDuration) = {

        def apply(state: State, command: Command) = {
          command match {
            case Command.AssignPartitions(partitions) =>
              state
                .actions
                .collectFirst { case action: Action.Subscribe =>
                  val topicPartitions = partitions.map { partition => TopicPartition(action.topic, partition) }
                  action
                    .listener
                    .onPartitionsAssigned(topicPartitions)
                    .run(state)
                    .map { case (s, _) => s }
                }
                .getOrElse(state)
                .map { state => (state, ConsumerRecords.empty[String, ByteVector].pure[Try]) }

            case Command.ProduceRecords(records) => (state, records.pure[Try])

            case Command.RevokePartitions(partitions) =>
              state
                .actions
                .collectFirst { case action: Action.Subscribe =>
                  val topicPartitions = partitions.map { partition => TopicPartition(action.topic, partition) }
                  action
                    .listener
                    .onPartitionsRevoked(topicPartitions)
                    .run(state)
                    .map { case (s, _) => s }
                }
                .getOrElse(state)
                .map { s => (s, ConsumerRecords.empty[String, ByteVector].pure[Try]) }

            case Command.Fail(error) => (state, error.raiseError[Try, ConsumerRecords[String, ByteVector]])
          }
        }

        StateT { state =>
          state.commands match {
            case Nil                 => (state, ConsumerRecords.empty[String, ByteVector].pure[Try])
            case command :: commands => apply(state.copy(commands = commands), command)
          }
        }
      }

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
        StateT.unit { _ + Action.Commit(offsets) }
      }
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


  def consumerRecord(
    partition: Partition,
    offset: Offset,
  ): ConsumerRecord[String, ByteVector] = {
    ConsumerRecord(
      topicPartition = TopicPartition(topic = topic, partition = partition),
      offset = offset,
      timestampAndType = none,
      key = WithSize(key).some,
      value = none,
      headers = List.empty)
  }

  def consumerRecords(records: ConsumerRecord[String, ByteVector]*): ConsumerRecords[String, ByteVector] = {
    Nel.fromList(records.toList).fold {
      ConsumerRecords.empty[String, ByteVector]
    } { records =>
      ConsumerRecords(records.groupBy(_.topicPartition))
    }
  }


  val subscriptionFlow: Stream[StateT, ConsumerRecords[String, ByteVector]] = {
    SubscriptionFlow(topic, consumer, topicFlowOf, retry)
  }


  sealed abstract class Command

  object Command {
    final case class ProduceRecords(consumerRecords: ConsumerRecords[String, ByteVector]) extends Command
    final case class AssignPartitions(partitions: Nel[Partition]) extends Command
    final case class RevokePartitions(partitions: Nel[Partition]) extends Command
    final case class Fail(error: Throwable) extends Command
  }

  sealed abstract class Action

  object Action {
    final case class AcquireTopicFlow(topic: Topic) extends Action
    final case class ReleaseTopicFlow(topic: Topic) extends Action
    case object AcquireConsumer extends Action
    case object ReleaseConsumer extends Action
    final case class AssignPartitions(partitions: Nel[Partition]) extends Action
    final case class RevokePartitions(partitions: Nel[Partition]) extends Action
    final case class Subscribe(topic: Topic)(val listener: RebalanceListener[StateT]) extends Action
    final case class Poll(consumerRecords: ConsumerRecords[String, ByteVector]) extends Action
    final case class Commit(offsets: Map[TopicPartition, OffsetAndMetadata]) extends Action
    final case class RetryOnError(error: Throwable, decision: OnError.Decision) extends Action
  }

  case object Error extends RuntimeException with NoStackTrace
}
