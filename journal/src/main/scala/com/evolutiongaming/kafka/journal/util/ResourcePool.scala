package com.evolutiongaming.kafka.journal.util

import cats.instances.all._
import cats.effect.implicits._
import cats.effect.kernel.{Async, Deferred}
import cats.effect.std.Mutex
import cats.effect.{Ref, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log

import java.util.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration
import scala.collection.immutable.Queue

private[journal] trait ResourcePool[F[_], O] {

  /**
   * `acquire` function of the Resource semantically blocks if there are no objects available.
   *
   * `release` function of the Resource returns the object to the pool.
   * Note that it will NOT deallocate the object.
   */
  def borrow: Resource[F, O]
}

private[journal] object ResourcePool {

  type ObjectId = Int
  type Waiter[F[_], O] = Deferred[F, Either[Throwable, (ObjectId, O)]]

  case class AllocatedObject[F[_], O](value: O, deallocate: F[Unit], state: AllocatedObject.State)

  object AllocatedObject {
    sealed abstract class State(val isFree: Boolean)

    object State {
      case object Used extends State(false)
      case class Free(releasedAt: FiniteDuration) extends State(true)
    }
  }

  case class State[F[_], O](
    objects: Map[ObjectId, AllocatedObject[F, O]],
    waiters: Queue[Waiter[F, O]],
  ) {

    def findFreeObject: Option[(ObjectId, AllocatedObject[F, O])] =
      objects.collectFirst {
        case (id, obj) if obj.state.isFree => (id, obj)
      }

    def markObjectAsUsed(id: ObjectId, obj: AllocatedObject[F, O]): State[F, O] =
      State(
        objects + (id -> obj.copy(state = AllocatedObject.State.Used)),
        waiters
      )

    def markObjectAsFree(id: ObjectId, obj: AllocatedObject[F, O], releasedAt: FiniteDuration): State[F, O] =
      State(
        objects + (id -> obj.copy(state = AllocatedObject.State.Free(releasedAt))),
        waiters
      )

    def addObject(id: ObjectId, obj: AllocatedObject[F, O]): State[F, O] =
      State(
        objects + (id -> obj),
        waiters
      )

    def removeObject(id: ObjectId): State[F, O] =
      State(
        objects - id,
        waiters
      )

    def addWaiter(waiter: Waiter[F, O]): State[F, O] =
      State(
        objects,
        waiters.enqueue(waiter)
      )

    def takeWaiter: (State[F, O], Option[Waiter[F, O]]) =
      waiters.headOption match {
        case Some(waiter) =>
          State(objects, waiters.tail) -> waiter.some
        case None =>
          State(objects, Queue.empty) -> none
      }
  }

  def fixedSize[F[_]: Async, O](
    poolSize: Int,
    acquireTimeout: FiniteDuration,
    idleTimeout: FiniteDuration,
    log: Log[F],
  )(
    resource: Resource[F, O]
  ): Resource[F, ResourcePool[F, O]] = {

    def poolImpl(mutex: Mutex[F], stateRef: Ref[F, State[F, O]], objCounter: Ref[F, Int]): ResourcePool[F, O] = {

      val allocateObject: F[(ObjectId, AllocatedObject[F, O])] =
        for {
          id <- objCounter.updateAndGet(_ + 1)
          a <- resource.allocated
          (obj, release) = a
        } yield id -> AllocatedObject(obj, release, AllocatedObject.State.Used)

      def handleNewWaiter(waiter: Waiter[F, O]): F[Unit] = {
        val timeout = waiter.complete(
          Left(new TimeoutException(s"Resource acquisition timed out"))
        )

        stateRef.get.flatMap { state =>
          state.findFreeObject match {
            case Some((id, obj)) =>
              for {
                _ <- waiter.complete((id, obj.value).asRight)
                _ <- stateRef.update(_.markObjectAsUsed(id, obj))
              } yield ()

            case None =>
              if (state.objects.size < poolSize) {
                for {
                  a <- allocateObject
                  (id, obj) = a
                  _ <- waiter.complete((id, obj.value).asRight)
                  _ <- stateRef.update(_.addObject(id, obj))
                } yield ()
              } else {
                for {
                  _ <- (Async[F].sleep(acquireTimeout) *> timeout).start
                  _ <- stateRef.update(_.addWaiter(waiter))
                } yield ()
              }
          }
        }
      }

      val acquireObject: F[(ObjectId, O)] =
        Async[F].uncancelable { _ =>
          for {
            waiter  <- Deferred[F, Either[Throwable, (ObjectId, O)]]
            _       <- mutex.lock.surround(handleNewWaiter(waiter))
            result  <- waiter.get.rethrow
          } yield result
        }

      def checkIfExpired(id: ObjectId, timeoutScheduledAt: FiniteDuration): F[Unit] = {
        mutex.lock.surround {
          stateRef.get.map(_.objects.get(id)).flatMap {
            case None =>
              log.error(s"Tried to check if resource $id was expired, but it was not found in the state. " +
                "This is not normal, please contact the maintainers.")

            case Some(AllocatedObject(_, deallocate, objState)) =>
              objState match {
                case AllocatedObject.State.Free(releasedAt) if releasedAt == timeoutScheduledAt =>
                  for {
                    _ <- deallocate.handleErrorWith { err =>
                      log.error(s"Failed to deallocate resource $id", err)
                    }.start

                    _ <- stateRef.update(_.removeObject(id))
                  } yield ()

                case _ =>
                  Async[F].unit
              }
          }
        }
      }

      def releaseObject(objId: ObjectId): F[Unit] = {

        // Drain the queue discarding timed out waiters and complete the first valid waiter with the object.
        // Returns true if a waiter was completed with the object;
        // returns false if the entire queue was drained but no valid waiters were available.
        def tryCompleteWaiter(
          objId: ObjectId,
          obj: AllocatedObject[F, O],
          initialState: State[F, O]
        ): F[(State[F, O], Boolean)] = {
          initialState.tailRecM { state =>
            state.takeWaiter match {
              case (s, None) =>
                // we've drained the queue
                (s, false).asRight[State[F, O]].pure[F]
              case (s, Some(waiter)) =>
                waiter
                  .complete((objId, obj.value).asRight)
                  .flatMap(_ => waiter.get)
                  .map {
                    case Right((`objId`, _)) =>
                      // this waiter has just been completed with our object
                      (s, true).asRight
                    case _ =>
                      // this waiter timed out and completed with an error
                      s.asLeft
                  }
            }
          }
        }

        mutex.lock.surround {
          stateRef.get.flatMap { state =>
            state.objects.get(objId) match {
              case None =>
                log.error(s"Resource $objId was returned to the pool but it was not found in the state. " +
                  "This is not normal, please contact the maintainers.")

              case Some(obj) =>
                tryCompleteWaiter(objId, obj, state).flatMap {
                  case (state, objectUsed) =>
                    if (objectUsed) {
                      stateRef.set(state)
                    } else {
                      for {
                        releasedAt  <- Async[F].monotonic
                        _           <- (Async[F].sleep(idleTimeout) *> checkIfExpired(objId, releasedAt)).start
                        _           <- stateRef.set(state.markObjectAsFree(objId, obj, releasedAt))
                      } yield ()
                    }
                }
            }
          }
        }
      }

      new ResourcePool[F, O] {
        override def borrow: Resource[F, O] = {
          val res = Resource.make(acquireObject) {
            case (id, _) =>
              releaseObject(id)
          }

          res.map { case (_, obj) => obj }
        }
      }
    }

    def shutdown(mutex: Mutex[F], stateRef: Ref[F, State[F, O]]): F[Unit] = {

      def deallocateObjects(state: State[F, O]) =
        state.objects.values.toVector.parTraverse_ {
          case AllocatedObject(_, deallocate, _) =>
            deallocate
        }

      def notifyWaiters(state: State[F, O]) =
        state.waiters.traverse_ {
          waiter =>
            waiter.complete(
              Left(new IllegalStateException("Pool is shutting down"))
            )
        }

      mutex.lock.surround {
        for {
          state <- stateRef.get
          _ <- notifyWaiters(state)
          _ <- deallocateObjects(state)
        } yield ()
      }
    }

    val res = Resource.make {
      for {
        mutex <- Mutex[F]
        stateRef <- Ref.of[F, State[F, O]](State(Map.empty, Queue.empty))
        objCounter <- Ref.of[F, Int](0)
      } yield (mutex, stateRef, objCounter)
    } {
      case (mutex, stateRef, _) =>
        shutdown(mutex, stateRef)
    }

    res.map {
      case (mutex, stateRef, objCounter) =>
        poolImpl(mutex, stateRef, objCounter)
    }
  }
}
