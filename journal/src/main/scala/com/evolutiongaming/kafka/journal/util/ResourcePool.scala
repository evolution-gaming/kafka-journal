package com.evolutiongaming.kafka.journal.util

import cats.instances.all._
import cats.effect.implicits._
import cats.effect.kernel.{Async, Deferred, Sync}
import cats.effect.std.Mutex
import cats.effect.{Ref, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration

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

  type ObjectId = UUID
  type WaiterId = UUID
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
    waiters: Map[WaiterId, Waiter[F, O]],
  )

  def fixedSize[F[_]: Async, O](
    resource: Resource[F, O],
    poolSize: Int,
    acquireTimeout: FiniteDuration,
    resourceTTL: FiniteDuration,
    log: Log[F],
  ): Resource[F, ResourcePool[F, O]] = {

    type StateRef = Ref[F, State[F, O]]

    def poolImpl(mutex: Mutex[F], stateRef: StateRef): ResourcePool[F, O] = {

      def findFreeObject(state: State[F, O]): Option[(ObjectId, AllocatedObject[F, O])] = {
        state.objects.collectFirst {
          case (id, obj) if obj.state.isFree => (id, obj)
        }
      }

      def markObjectAsUsed(id: ObjectId, obj: AllocatedObject[F, O]): F[Unit] = {
        stateRef.update { state =>
          state.copy(objects = state.objects + (id -> obj.copy(state = AllocatedObject.State.Used)))
        }
      }

      def allocateObject(state: State[F, O]): F[(ObjectId, O)] = {
        for {
          a <- resource.allocated
          (obj, release) = a
          objId <- Sync[F].delay(UUID.randomUUID())
          state1 = state.copy(
            objects = state.objects + (objId -> AllocatedObject(obj, release, AllocatedObject.State.Used))
          )
          _ <- stateRef.set(state1)
        } yield (objId, obj)
      }

      def checkForAcquireTimeout(
        waiterId: WaiterId,
        promise: Waiter[F, O]
      ): F[Unit] = {
        promise.tryGet.flatMap {
          case Some(_) =>
            Async[F].unit
          case None =>
            mutex.lock.surround {
              promise.tryGet.flatMap {
                case Some(_) =>
                  Async[F].unit
                case None =>
                  for {
                    _ <- promise.complete(
                      Left(
                        new TimeoutException(s"Resource acquisition timed out")
                      )
                    )
                    _ <- stateRef.update { state =>
                      state.copy(waiters = state.waiters - waiterId)
                    }
                  } yield ()
              }
            }
        }
      }

      def registerWaiter(state: State[F, O], promise: Waiter[F, O]): F[Unit] = {
        for {
          waiterId <- Sync[F].delay(UUID.randomUUID())
          _ <- stateRef.set {
            state.copy(
              waiters = state.waiters + (waiterId -> promise)
            )
          }

          _ <- (Async[F].sleep(acquireTimeout) *> checkForAcquireTimeout(waiterId, promise)).start
        } yield ()
      }

      val acquirePromise: F[Waiter[F, O]] =
        Deferred[F, Either[Throwable, (ObjectId, O)]].flatMap { promise =>
          mutex.lock.surround {
            stateRef.get.flatMap { state =>
              findFreeObject(state) match {
                case Some((id, obj)) =>
                  for {
                    _ <- markObjectAsUsed(id, obj)
                    _ <- promise.complete((id, obj.value).asRight)
                  } yield ()

                case None =>
                  if (state.objects.size < poolSize) {
                    for {
                      o <- allocateObject(state)
                      _ <- promise.complete(o.asRight)
                    } yield ()
                  } else {
                    registerWaiter(state, promise)
                  }
              }
            }
          }.as(promise)
        }

      val acquireObject: F[(ObjectId, O)] = {
        acquirePromise.flatMap { promise =>
          promise.get.flatMap {
            case Right(x) => x.pure[F]
            case Left(err) => Async[F].raiseError(err)
          }
        }
      }

      def checkIfExpired(id: ObjectId, timeoutScheduledAt: FiniteDuration): F[Unit] = {
        mutex.lock.surround {
          stateRef.get.map(_.objects.get(id)).flatMap {
            case Some(AllocatedObject(_, deallocate, AllocatedObject.State.Free(releasedAt))) =>
              if (releasedAt == timeoutScheduledAt) {
                for {
                  _ <- deallocate.handleErrorWith { err =>
                    log.error(s"Failed to deallocate resource $id", err)
                  }.start

                  _ <- stateRef.update { state =>
                    state.copy(objects = state.objects - id)
                  }
                } yield ()
              } else {
                Async[F].unit
              }

            case o =>
              log.error(
                s"Resource $id is in unexpected state ${o.map(_.state)}. This is not normal, please contact the maintainers."
              )
          }
        }
      }

      def releaseObject(id: ObjectId): F[Unit] =
        mutex.lock.surround {
          stateRef.get.flatMap { state =>
            state.objects.get(id) match {
              case None =>
                log.error(s"Resource $id was returned to the pool but it was not found in the state. " +
                  "This is not normal, please contact the maintainers.")

              case Some(obj) =>
                state.waiters.headOption match {
                  case Some((waiterId, waiter)) =>
                    for {
                      _ <- waiter.complete((id, obj.value).asRight[Throwable])
                      _ <- stateRef.update { state =>
                        state.copy(waiters = state.waiters - waiterId)
                      }
                      _ <- markObjectAsUsed(id, obj)
                    } yield ()

                  case None =>
                    for {
                      releasedAt <- Async[F].monotonic
                      _ <- (Async[F].sleep(resourceTTL) *> checkIfExpired(id, releasedAt)).start
                      _ <- stateRef.update { state =>
                        state.copy(
                          objects = state.objects.updated(
                            id,
                            obj.copy(state = AllocatedObject.State.Free(releasedAt))
                          )
                        )
                      }
                    } yield ()
                }
            }
          }
        }

      class Main
      new Main with ResourcePool[F, O] {
        override def borrow: Resource[F, O] = {
          val res = Resource.make(acquireObject) {
            case (id, _) =>
              releaseObject(id)
          }

          res.map { case (_, obj) => obj }
        }
      }
    }

    def shutdown(mutex: Mutex[F], state: StateRef): F[Unit] = {

      def deallocateObjects(state: State[F, O]) =
        state.objects.values.toVector.parTraverse_ {
          case AllocatedObject(_, release, _) =>
            release
        }

      def notifyWaiters(state: State[F, O]) =
        state.waiters.values.toVector.parTraverse_ {
          promise =>
            promise.complete(
              Left(new IllegalStateException("Pool is shutting down"))
            )
        }

      mutex.lock.surround {
        for {
          state <- state.get
          _ <- notifyWaiters(state)
          _ <- deallocateObjects(state)
        } yield ()
      }
    }

    val res = Resource.make {
      for {
        mutex <- Mutex[F]
        stateRef <- Ref.of[F, State[F, O]](State(Map.empty, Map.empty))
      } yield (mutex, stateRef)
    } {
      case (mutex, stateRef) =>
        shutdown(mutex, stateRef)
    }

    res.map {
      case (mutex, stateRef) =>
        poolImpl(mutex, stateRef)
    }
  }
}
