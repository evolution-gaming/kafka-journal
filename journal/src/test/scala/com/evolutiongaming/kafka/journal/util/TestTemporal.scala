package com.evolutiongaming.kafka.journal.util

import cats.Monad
import cats.arrow.FunctionK
import cats.effect._
import cats.effect.kernel.{Poll, Unique}
import cats.syntax.all._

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

object TestTemporal {
  type StateT[S, A] = cats.data.StateT[IO, S, A]

  type StateTry[S, A] = cats.data.StateT[IO, S, Try[A]]

  implicit def temporal[S]: Temporal[StateT[S, *]] = new Temporal[StateT[S, *]] {

    def of[A](f: S => IO[(S, A)]): StateT[S, A] = cats.data.StateT[IO, S, A](data => f(data))

    import cats.data.{StateT => St}

    type ST[B] = StateT[S, B]

    override def sleep(time: FiniteDuration): StateT[S, Unit] = St.liftF(IO.sleep(time))

    override def monotonic: StateT[S, FiniteDuration] = St.liftF(IO.monotonic)

    override def realTime: StateT[S, FiniteDuration] = St.liftF(IO.realTime)

    override def ref[A](a: A): ST[Ref[ST, A]] = St.liftF {
      Ref.of[IO, A](a).map(_.mapK[ST](new FunctionK[IO, ST] {
        override def apply[X](fa: IO[X]): ST[X] = St.liftF(fa)
      }))
    }

    override def deferred[A1]: ST[Deferred[ST, A1]] = St.liftF {
      Deferred[IO, A1].map(_.mapK[ST](new FunctionK[IO, ST] {
        override def apply[X](fa: IO[X]): ST[X] = St.liftF(fa)
      }))
    }


    // We don't allow parallelism as parallel fibers will all have their own state and it will become inconsistent
    override def start[A](fa: StateT[S, A]): StateT[S, Fiber[StateT[S, *], Throwable, A]] = of { s =>
      val stateFiber = new Fiber[StateT[S, *], Throwable, A] {
        val cancelled = new AtomicBoolean(false)

        override def cancel: StateT[S, Unit] = St.liftF(IO.delay(cancelled.set(true)))

        override def join: StateT[S, Outcome[StateT[S, *], Throwable, A]] = {
          if (cancelled.get()) St.pure(Outcome.canceled[StateT[S, *], Throwable, A])
          else fa.map(a => Outcome.succeeded[StateT[S, *], Throwable, A](pure(a))).handleErrorWith(e => raiseError(e))
        }
      }

      IO.pure((s, stateFiber))
    }


    override def racePair[A, B](fa: StateT[S, A], fb: StateT[S, B]): StateT[S, Either[
      (Outcome[StateT[S, *], Throwable, A], Fiber[StateT[S, *], Throwable, B]),
      (Fiber[StateT[S, *], Throwable, A], Outcome[StateT[S, *], Throwable, B])
    ]] =
      for {
        fa <- start(fa)
        fb <- start(fb)
        result <- fa.join.map(outcome => Left((outcome, fb)))
      } yield result

    override def never[A]: StateT[S, A] = St.liftF(IO.never)

    override def cede: StateT[S, Unit] = St.liftF(IO.cede)

    override def forceR[A, B](fa: StateT[S, A])(fb: StateT[S, B]): StateT[S, B] = of { s =>
      fa.run(s).redeemWith(_ => fb.run(s), r => fb.run(r._1))
    }

    override def uncancelable[A](body: Poll[StateT[S, *]] => StateT[S, A]): StateT[S, A] = body(new Poll[StateT[S, *]] {
      override def apply[X](fa: StateT[S, X]): StateT[S, X] = of { s: S =>
        fa.run(s).uncancelable
      }
    })

    override def canceled: StateT[S, Unit] = St.liftF(IO.canceled)

    override def onCancel[A](fa: StateT[S, A], fin: StateT[S, Unit]): StateT[S, A] = of { s =>
      fa.run(s).onCancel(fin.run(s).map(_._2))
    }

    override def raiseError[A](e: Throwable): StateT[S, A] = of(_ => IO.raiseError(e))

    override def handleErrorWith[A](fa: StateT[S, A])(f: Throwable => StateT[S, A]): StateT[S, A] = of { s =>
      fa.run(s).handleErrorWith(e => f(e).run(s))
    }

    override def flatMap[A, B](fa: StateT[S, A])(f: A => StateT[S, B]): StateT[S, B] = St { s =>
      fa.run(s).flatMap {
        case (s1, a1) => f(a1).run(s1)
      }
    }

    override def tailRecM[A, B](a: A)(f: A => StateT[S, Either[A, B]]): StateT[S, B] = of { s =>
      Monad[IO].tailRecM((a, s)) {
        case (a1, s1) =>
          f(a1).run(s1).map {
            case (s2, Left(a2)) => Left((a2, s2))
            case (s2, Right(b)) => Right((s2, b))
          }
      }
    }

    override def pure[A](x: A): StateT[S, A] = St.pure(x)

    override def unique: StateT[S, Unique.Token] = St.liftF(IO.unique)
  }

  implicit def temporalTry[S]: Temporal[StateTry[S, *]] = new Temporal[StateTry[S, *]] {
    def of[A](f: S => IO[(S, Try[A])]): StateTry[S, A] = cats.data.StateT[IO, S, Try[A]](data => f(data))

    import cats.data.{StateT => St}

    type ST[B] = StateTry[S, B]

    override def sleep(time: FiniteDuration): ST[Unit] = St.liftF(IO.sleep(time).as(Success(())))

    override def monotonic: ST[FiniteDuration] = St.liftF(IO.monotonic.map(Success(_)))

    override def realTime: ST[FiniteDuration] = St.liftF(IO.realTime.map(Success(_)))

    override def ref[A](a: A): ST[Ref[ST, A]] = St.liftF {
      Ref.of[IO, A](a).map(_.mapK[ST](new FunctionK[IO, ST] {
        override def apply[X](fa: IO[X]): ST[X] = St.liftF(fa.map(Success(_)))
      }).pure[Try])
    }

    override def deferred[A1]: ST[Deferred[ST, A1]] = St.liftF {
      Deferred[IO, A1].map(_.mapK[ST](new FunctionK[IO, ST] {
        override def apply[X](fa: IO[X]): ST[X] = St.liftF(fa.map(Success(_)))
      })).map(Success(_))
    }

    // We don't allow parallelism as parallel fibers will all have their own state and it will become inconsistent
    override def start[A](fa: ST[A]): ST[Fiber[ST, Throwable, A]] = of { s =>
      val stateFiber = new Fiber[ST, Throwable, A] {
        override def cancel: ST[Unit] = St.liftF(IO.unit.as(Success(())))

        override def join: ST[Outcome[ST, Throwable, A]] = {
          fa.map(a => Success(Outcome.succeeded[ST, Throwable, A](of(s => IO.pure((s, a))))))
        }
      }

      IO.pure((s, Success(stateFiber)))
    }

    override def never[A]: ST[A] = St.liftF(IO.never)

    override def cede: ST[Unit] = St.liftF(IO.cede.as(Success(())))

    override def forceR[A, B](fa: ST[A])(fb: ST[B]): ST[B] =
      of(s => fa.run(s).redeemWith(_ => fb.run(s), r => fb.run(r._1)))

    override def uncancelable[A](body: Poll[StateTry[S, *]] => StateTry[S, A]): StateTry[S, A] =
      body(new Poll[StateTry[S, *]] {
        override def apply[X](fa: ST[X]): ST[X] = fa
      })

    override def canceled: ST[Unit] = St.liftF(IO.canceled.as(Success(())))

    override def onCancel[A](fa: ST[A], fin: ST[Unit]): ST[A] = of(s => fa.run(s).onCancel(fin.run(s).map(_._2).void))

    override def raiseError[A](e: Throwable): ST[A] = St(s => IO.pure((s, Failure(e))))

    override def handleErrorWith[A](fa: ST[A])(f: Throwable => ST[A]): ST[A] = of { s =>
      fa.run(s).flatMap {
        case (s, Success(value)) =>
          IO.pure((s, Success(value)))
        case (s, Failure(e)) =>
          f(e).run(s)
      }
    }

    override def flatMap[A, B](fa: ST[A])(f: A => ST[B]): ST[B] =
      fa.flatMap {
        case Failure(e) => St.pure(Failure(e))
        case Success(value) => f(value)
      }

    override def tailRecM[A, B](a: A)(f: A => ST[Either[A, B]]): ST[B] = of { s =>
      (a, s).tailRecM { case (a1, s1) =>
        f(a1).run(s1).map {
          case (s2, Success(Left(a2))) => Left((a2, s2))
          case (s2, Success(Right(b))) => Right((s2, Success(b)))
          case (s2, Failure(e)) => Right((s2, Failure(e)))
        }
      }
    }

    override def pure[A](x: A): ST[A] = St.pure(Success(x))

    override def unique: ST[Unique.Token] = St.liftF(IO.unique.map(Success(_)))
  }
}
