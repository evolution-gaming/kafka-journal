package com.evolutiongaming.kafka.journal.stream

import cats.data.IndexedStateT
import cats.effect.{Bracket, ExitCase, Resource}
import cats.implicits._
import cats.{Id, MonadError}
import org.scalatest.{FunSuite, Matchers}

import scala.util.{Success, Try}

class StreamSpec extends FunSuite with Matchers {

  test("apply resource") {

    sealed trait Action

    object Action {
      case object Acquire extends Action
      case object Release extends Action
      case object Use extends Action
    }

    case class State(n: Int, actions: List[Action]) {
      def add(action: Action): State = copy(actions = action :: actions)
    }

    object State {
      lazy val Empty: State = State(0, List.empty)
    }


    type StateT[A] = cats.data.StateT[Try, State, A]

    object StateT {

      def apply[A](f: State => (State, A)): StateT[A] = {
        cats.data.StateT[Try, State, A] { state => Success(f(state)) }
      }
    }


    def bracketOf[F[_]](implicit F: MonadError[F, Throwable]): Bracket[F, Throwable] = new Bracket[F, Throwable] {

      def bracketCase[A, B](acquire: F[A])(use: A => F[B])(release: (A, ExitCase[Throwable]) => F[Unit]) = {

        def onError(a: A, error: Throwable) = {
          for {
            _ <- release(a, ExitCase.error(error))
            b <- raiseError[B](error)
          } yield b
        }

        for {
          a <- acquire
          b <- use(a).handleErrorWith(e => onError(a, e))
          _ <- release(a, ExitCase.complete)
        } yield b
      }

      def raiseError[A](e: Throwable) = F.raiseError(e)

      def handleErrorWith[A](fa: F[A])(f: Throwable => F[A]) = F.handleErrorWith(fa)(f)

      def flatMap[A, B](fa: F[A])(f: A => F[B]) = F.flatMap(fa)(f)

      def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]) = F.tailRecM(a)(f)

      def pure[A](a: A) = F.pure(a)
    }


    implicit val bracket = bracketOf[StateT](IndexedStateT.catsDataMonadErrorForIndexedStateT[Try, State, Throwable])

    val increment = StateT { state =>
      val n = state.n + 1
      val state1 = state.copy(n = n).add(Action.Use)
      (state1, n)
    }

    val resource = Resource.make {
      StateT { state =>
        val state1 = state.add(Action.Acquire)
        (state1, increment)
      }
    } { _ =>
      StateT { state =>
        val state1 = state.add(Action.Release)
        (state1, ())
      }
    }

    val stream = for {
      a <- Stream[StateT].apply(resource)
      a <- Stream[StateT].repeat(a)
    } yield a

    val (state, value) = stream.take(2).toList.run(State.Empty).get
    value shouldEqual List(1, 2)
    state shouldEqual State(3, List(
      Action.Release,
      Action.Use,
      Action.Use,
      Action.Use,
      Action.Acquire))
  }

  test("lift") {
    Stream.lift[Id, Int](0).toList shouldEqual List(0)
  }

  test("single") {
    Stream[Id].single(0).toList shouldEqual List(0)
  }

  test("empty") {
    Stream.empty[Id, Int].toList shouldEqual Nil
  }

  test("map") {
    Stream.lift[Id, Int](0).map(_ + 1).toList shouldEqual List(1)
  }

  test("flatMap") {
    val stream = Stream.lift[Id, Int](1)
    val stream1 = for {
      a <- stream
      b <- stream
    } yield a + b

    stream1.toList shouldEqual List(2)
  }

  test("take") {
    Stream.lift[Id, Int](0).take(3).toList shouldEqual List(0)
    Stream[Id].many(1, 2, 3).take(1).toList shouldEqual List(1)
  }

  test("first") {
    Stream[Id].single(0).first shouldEqual Some(0)
    Stream.empty[Id, Int].first shouldEqual None
  }

  test("last") {
    Stream[Id].many(1, 2, 3).last shouldEqual Some(3)
    Stream.empty[Id, Int].last shouldEqual None
  }

  test("length") {
    Stream.repeat[Id, Int](0).take(3).length shouldEqual 3
  }

  test("repeat") {
    Stream.repeat[Id, Int](0).take(3).toList shouldEqual List.fill(3)(0)
  }

  test("filter") {
    Stream[Id].many(1, 2, 3).filter(_ >= 2).toList shouldEqual List(2, 3)
  }

  test("collect") {
    Stream[Id].many(1, 2, 3).collect { case x if x >= 2 => x + 1 }.toList shouldEqual List(3, 4)
  }

  test("zipWithIndex") {
    Stream.repeat[Id, Int](0).zipWithIndex.take(3).toList shouldEqual List((0, 0), (0, 1), (0, 2))
  }

  test("takeWhile") {
    Stream[Id].many(1, 2, 1).takeWhile(_ < 2).toList shouldEqual List(1)
  }

  test("dropWhile") {
    Stream[Id].many(1, 2, 1).dropWhile(_ < 2).toList shouldEqual List(2, 1)
  }
}
