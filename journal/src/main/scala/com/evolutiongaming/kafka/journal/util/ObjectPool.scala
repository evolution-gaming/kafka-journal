package com.evolutiongaming.kafka.journal.util

import cats.instances.all._
import cats.effect.implicits._
import cats.effect.kernel.{Async, Clock, Deferred}
import cats.effect.std.Mutex
import cats.effect.{Fiber, Ref, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log

import scala.concurrent.duration._
import scala.collection.immutable.Queue
import java.time.Instant

private[journal] trait ObjectPool[F[_], O] {

  /**
   * `acquire` function of the Resource semantically blocks if there are no objects available.
   *
   * `release` function of the Resource returns the object to the pool.
   * Note that it will NOT deallocate the object.
   */
  def borrow: Resource[F, O]
}

private[journal] object ObjectPool {

  type ObjectId = Int

  // Invariant: waiters are only completed inside sections of code protected by mutex
  type Waiter[F[_], O] = Deferred[F, (ObjectId, O)]

  case class AllocatedObject[F[_], O](value: O, deallocate: F[Unit])

  case class State[F[_], O](
    objects: Map[ObjectId, AllocatedObject[F, O]],
    freeObjects: Queue[(ObjectId, Instant)],
    waiters: Queue[Waiter[F, O]],
  ) {

    def getFreeObject: Option[ObjectId] =
      freeObjects.headOption.map(_._1)

    def getObjectOrRaise(id: ObjectId)(implicit F: Async[F]): F[AllocatedObject[F, O]] =
      objects.get(id) match {
        case Some(o) => o.pure[F]
        case None =>
          Async[F].raiseError(
            new IllegalStateException(
              s"Object $id was not found in the pool. There's a bug in kafka-journal, please contact the maintainers."
            )
          )
      }

    def markFirstFreeObjectAsUsed: State[F, O] =
      State(
        objects,
        freeObjects.tail,
        waiters
      )

    def markObjectAsFree(id: ObjectId, releasedAt: Instant): State[F, O] =
      State(
        objects,
        freeObjects.enqueue((id, releasedAt)),
        waiters
      )

    def addObject(id: ObjectId, obj: AllocatedObject[F, O]): State[F, O] =
      State(
        objects + (id -> obj),
        freeObjects,
        waiters
      )

    def findIdleObjects(now: Instant, idleTimeout: FiniteDuration): Queue[ObjectId] = {
      freeObjects.collect {
        case (objId, releasedAt) if (now.toEpochMilli - releasedAt.toEpochMilli >= idleTimeout.toMillis) =>
          objId
      }
    }

    def removeIdleObjects(ids: Set[ObjectId]): State[F, O] =
      State(
        objects.filter {
          case (objId, _) =>
            !ids.contains(objId)
        },
        freeObjects.filter(o => !ids.contains(o._1)),
        waiters
      )

    def addWaiter(waiter: Waiter[F, O]): State[F, O] =
      State(
        objects,
        freeObjects,
        waiters.enqueue(waiter)
      )

    def takeWaiter: (State[F, O], Option[Waiter[F, O]]) =
      waiters.headOption match {
        case Some(waiter) =>
          State(objects, freeObjects, waiters.tail) -> waiter.some
        case None =>
          State(objects, freeObjects, Queue.empty) -> none
      }
  }

  def fixedSize[F[_]: Async, O](
    poolSize: Int,
    idleTimeout: FiniteDuration,
    log: Log[F],
  )(
    resource: Resource[F, O]
  ): Resource[F, ObjectPool[F, O]] = {

    def mainPoolLogic(mutex: Mutex[F], stateRef: Ref[F, State[F, O]], objCounter: Ref[F, Int]): ObjectPool[F, O] = {

      val allocateObject: F[(ObjectId, AllocatedObject[F, O])] =
        for {
          id <- objCounter.updateAndGet(_ + 1)
          a <- resource.allocated
          (obj, deallocate) = a
        } yield id -> AllocatedObject(obj, deallocate)

      def handleNewWaiter(waiter: Waiter[F, O]): F[Unit] = {
        stateRef.get.flatMap { state =>
          state.getFreeObject match {
            case Some(id) =>
              for {
                obj <- state.getObjectOrRaise(id)
                _ <- waiter.complete((id, obj.value))
                _ <- stateRef.update(_.markFirstFreeObjectAsUsed)
              } yield ()

            case None =>
              if (state.objects.size < poolSize) {
                for {
                  a <- allocateObject
                  (id, obj) = a
                  _ <- waiter.complete((id, obj.value))
                  _ <- stateRef.update(_.addObject(id, obj))
                } yield ()
              } else {
                stateRef.update(_.addWaiter(waiter))
              }
          }
        }
      }

      val acquireObject: F[(ObjectId, O)] =
        for {
          waiter  <- Deferred[F, (ObjectId, O)]
          _       <- mutex.lock.surround(handleNewWaiter(waiter))
          result  <- waiter.get
        } yield result

      def releaseObject(objId: ObjectId): F[Unit] = {
        mutex.lock.surround {
          stateRef.get.flatMap { state =>
            state.getObjectOrRaise(objId).flatMap { obj =>
              state.takeWaiter match {
                case (state, Some(waiter)) =>
                  waiter.complete((objId, obj.value)) *> stateRef.set(state)
                case (state, None) =>
                  Clock[F].realTimeInstant.flatMap { now =>
                    stateRef.set(state.markObjectAsFree(objId, now))
                  }
              }
            }
          }
        }
      }

      new ObjectPool[F, O] {
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
      mutex.lock.surround {
        stateRef.get.flatMap { state =>
          state.objects.values.toVector.parTraverse_ {
            case AllocatedObject(_, deallocate) =>
              deallocate
          }
        }
      }
    }

    def checkForIdleObjects(mutex: Mutex[F], stateRef: Ref[F, State[F, O]]): F[Unit] = {
      Clock[F].realTimeInstant.flatMap { now =>
        stateRef.get.map(_.findIdleObjects(now, idleTimeout)).flatMap { idleObjects =>
          if (idleObjects.isEmpty) {
            Async[F].unit
          } else {
            mutex.lock.surround {
              stateRef.get.flatMap { state =>
                val idleObjects = state.findIdleObjects(now, idleTimeout)
                if (idleObjects.isEmpty) {
                  Async[F].unit
                } else {
                  stateRef.set(state.removeIdleObjects(idleObjects.toSet)).flatMap { _ =>
                    idleObjects.parTraverse_ { id =>
                      state.getObjectOrRaise(id).flatMap {
                        case AllocatedObject(_, deallocate) =>
                          deallocate.handleErrorWith { ex =>
                            log.error(s"Failed to deallocate idle object $id", ex)
                          }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    def startCheckIdleObjects(mutex: Mutex[F], stateRef: Ref[F, State[F, O]]): F[Fiber[F, Throwable, Boolean]] =
      (Async[F].sleep(1.second) *> checkForIdleObjects(mutex, stateRef)).foreverM[Boolean].start

    val res = Resource.make {
      for {
        mutex <- Mutex[F]
        stateRef <- Ref.of[F, State[F, O]](State(Map.empty, Queue.empty, Queue.empty))
        objCounter <- Ref.of[F, Int](0)
        checkIdleObjectsFiber <- startCheckIdleObjects(mutex, stateRef)
      } yield (mutex, stateRef, objCounter, checkIdleObjectsFiber)
    } {
      case (mutex, stateRef, _, checkIdleObjectsFiber) =>
        for {
          _ <- checkIdleObjectsFiber.cancel
          _ <- shutdown(mutex, stateRef)
        } yield ()
    }

    res.map {
      case (mutex, stateRef, objCounter, _) =>
        mainPoolLogic(mutex, stateRef, objCounter)
    }
  }
}
