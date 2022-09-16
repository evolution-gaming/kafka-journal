package com.evolutiongaming.kafka.journal

import cats._
import cats.data.{NonEmptyList => Nel}
import cats.effect._
import cats.effect.syntax.all._
import cats.kernel.CommutativeMonoid
import cats.syntax.all._
import com.evolutiongaming.catshelper._
import com.evolutiongaming.kafka.journal.util.CacheHelper._
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.scache.Cache
import com.evolutiongaming.skafka.Offset

import scala.concurrent.duration.FiniteDuration


trait PartitionCache[F[_]] {

  def get(id: String, offset: Offset): F[PartitionCache.Result[F]]

  def offset: F[Option[Offset]]

  def add(records: Nel[PartitionCache.Record]): F[Option[PartitionCache.Diff]]

  def remove(offset: Offset): F[Option[PartitionCache.Diff]]

  def meters: F[PartitionCache.Meters]
}

object PartitionCache {

  def of[F[_]: Async: Runtime: Parallel](
    maxSize: Int = 10000,
    dropUponLimit: Double = 0.1,
    timeout: FiniteDuration
  ): Resource[F, PartitionCache[F]] = {
    main(
      maxSize = maxSize.max(1),
      dropUponLimit = dropUponLimit.max(0.01).min(1.0),
      timeout = timeout)
  }

  private def main[F[_]: Async: Runtime: Parallel](
    maxSize: Int,
    dropUponLimit: Double,
    timeout: FiniteDuration
  ): Resource[F, PartitionCache[F]] = {

    final case class Key(id: String, offset: Offset)

    for {
      stateRef <- Resource.make {
        Ref[F].of(State(offset = none, entries = none).asRight[Throwable])
      } { ref =>
        ref.set(ReleasedError.asLeft)
      }
      cache    <- Cache.loading1[F, Key, Listener[F]]
    } yield {

      class Main
      new Main with PartitionCache[F] {

        def get(id: String, offset: Offset) = {

          def listener: F[Listener[F]] = {
            val key = Key(id, offset)
            cache
              .getOrUpdateResource(key) {
                for {
                  deferred <- Resource.make {
                    Deferred[F, Either[Throwable, Result.Now]]
                  } { deferred =>
                    deferred
                      .complete(ReleasedError.asLeft)
                      .void
                  }
                  complete = (result: Result.Now) => {
                    deferred
                      .complete(result.asRight)
                      .flatMap {
                        case true  => cache.remove(key).void
                        case false => ().pure[F]
                      }
                  }
                  _ <- Temporal[F]
                    .sleep(timeout)
                    .productR { complete(Result.Now.timeout(timeout)) }
                    .background
                } yield {

                  new Listener[F] {

                    def get = {
                      deferred
                        .get
                        .rethrow
                    }

                    def updated(state: State) = {
                      state
                        .result(id, offset)
                        .foldMapM { result => complete(result) }
                    }
                  }
                }
              }
          }

          stateRef
            .get
            .rethrow
            .flatMap { state =>
              state.result(id, offset) match {
                case Right(a)    =>
                  a
                    .toResult[F]
                    .pure[F]
                case Left(entries) =>
                  listener.map { listener =>
                    entries match {
                      case Some(_) => Result.behind(listener.get)
                      case None    => Result.empty(listener.get)
                    }
                  }
              }
            }
        }

        def offset = {
          stateRef
            .get
            .rethrow
            .map { state =>
              state
                .entries
                .map { entries =>
                  entries
                    .bounds
                    .max
                }
                .max(state.offset)
            }
        }

        def add(records: Nel[Record]) = {
          for {
            bounds   <- Bounds.of[F](
              min = records
                .minimumBy { _.offset }
                .offset,
              max = records
                .maximumBy { _.offset }
                .offset
            )
            values   = for {
              (id, values) <- records
                .toList
                .collect { case Record(offset, Some(data)) => (offset, data) }
                .groupBy { case (_, record) => record.id }
              (offset, _)    = values.maxBy { case (offset, _) => offset }
              info           = values.foldLeft(HeadInfo.empty) { case (info, (offset, data)) => info(data.header, offset) }
              entry        <- info match {
                case HeadInfo.Empty       => none[Entry]
                case a: HeadInfo.NonEmpty => Entry(offset = offset, a).some
              }
            } yield {
              (id, entry)
            }
            entries  = Entries(bounds = bounds, values = values.toMap)

            result  <- 0.tailRecM { counter =>
              stateRef
                .access
                .flatMap { case (state, set) =>
                  state
                    .liftTo[F]
                    .flatMap { state =>
                      val entriesNotLimited = state
                        .entries
                        .fold(entries) { _.combine(entries) }
                      entriesNotLimited
                        .limit(maxSize, dropUponLimit)
                        .flatMap { entries =>
                          val state1 = state
                            .copy(entries = entries)
                            .asRight
                          set(state1).flatMap {
                            case true  =>
                              cache
                                .foldMapPar1 { _.updated(state.copy(entries = entriesNotLimited.some)) }
                                .as {
                                  state
                                    .entries
                                    .flatMap { entries =>
                                      Diff.of(
                                        prev = entries.bounds.max,
                                        next = bounds.max)
                                    }
                                    .asRight[Int]
                                }
                            case false =>
                              (counter + 1)
                                .asLeft[Option[Diff]]
                                .pure[F]
                          }
                        }
                    }
                }
            }
          } yield result
        }

        def remove(offset: Offset) = {
          0.tailRecM { counter =>
            stateRef
              .access
              .flatMap { case (state, set) =>
                state
                  .liftTo[F]
                  .flatMap { state =>
                    if (state.ahead(offset)) {
                      none[Diff]
                        .asRight[Int]
                        .pure[F]
                    } else {
                      state
                        .entries
                        .flatTraverse { entries =>
                          val bounds = entries.bounds
                          if (offset >= bounds.min) {
                            if (offset < bounds.max) {
                              for {
                                offset <- offset.inc[F]
                                bounds <- bounds.withMin(offset)
                              } yield {
                                val values = entries
                                  .values
                                  .filter { case (_, entry) => entry.offset >= bounds.min }
                                Entries(bounds = bounds, values = values).some
                              }
                            } else {
                              none[Entries].pure[F]
                            }
                          } else {
                            entries
                              .some
                              .pure[F]
                          }
                        }
                        .flatMap { entries =>
                          val state1 = State(offset = offset.some, entries = entries)
                          set(state1.asRight).flatMap {
                            case true  =>
                              cache
                                .foldMapPar1 { _.updated(state1) }
                                .as {
                                  state
                                    .offset
                                    .flatMap { offset0 => Diff.of(prev = offset0, next = offset) }
                                    .asRight[Int]
                                }
                            case false =>
                              (counter + 1)
                                .asLeft[Option[Diff]]
                                .pure[F]
                          }
                        }
                    }
                  }
              }
          }
        }

        def meters = {
          for {
            listeners <- cache.size
            state     <- stateRef.get
          } yield {
            Meters(
              listeners = listeners,
              entries = state.foldMap { _.entries.foldMap { _.values.size } })
          }
        }
      }
    }
  }

  sealed trait Result[+F[_]]

  object Result {

    def value[F[_]](value: HeadInfo): Result[F] = Now.value(value)

    def ahead[F[_]]: Result[F] = Now.ahead

    def limited[F[_]]: Result[F] = Now.limited

    def timeout[F[_]](duration: FiniteDuration): Result[F] = Now.timeout(duration)

    def behind[F[_]](value: F[Now]): Result[F] = Later.behind(value)

    def empty[F[_]](value: F[Now]): Result[F] = Later.empty(value)

    sealed trait Now extends Result[Nothing]

    object Now {
      def value(value: HeadInfo): Now = Value(value)

      def ahead: Now = Ahead

      def limited: Now = Limited

      def timeout(duration: FiniteDuration): Now = Timeout(duration)

      final case class Value(value: HeadInfo) extends Now

      final case object Ahead extends Now

      final case object Limited extends Now

      final case class Timeout(duration: FiniteDuration) extends Now

      implicit class NowOps(val self: Now) extends AnyVal {

        def toResult[F[_]]: Result[F] = self
      }
    }

    sealed trait Later[F[_]] extends Result[F]

    object Later {

      def behind[F[_]](value: F[Now]): Result[F] = Behind(value)

      def empty[F[_]](value: F[Now]): Result[F] = Empty(value)

      final case class Behind[F[_]](value: F[Now]) extends Later[F]

      final case class Empty[F[_]](value: F[Now]) extends Later[F]

      implicit class LaterOps[F[_]](val self: Later[F]) extends AnyVal {
        def value: F[Now] = self match {
          case Behind(a) => a
          case Empty(a)  => a
        }
      }
    }

    implicit class ResultOps[F[_]](val self: Result[F]) extends AnyVal {
      def toNow(implicit F: Monad[F]): F[Now] = {
        self match {
          case a: Now      => a.pure[F]
          case a: Later[F] => a.value
        }
      }
    }
  }

  private trait Listener[F[_]] {
    def get: F[Result.Now]

    def updated(state: State): F[Unit]
  }

  final case class Meters(listeners: Int, entries: Int)

  object Meters {

    val Empty: Meters = Meters(0, 0)

    implicit val commutativeMonoidMeters: CommutativeMonoid[Meters] = new CommutativeMonoid[Meters] {
      def empty = Empty
      def combine(a: Meters, b: Meters) = {
        Meters(
          listeners = a.listeners + b.listeners,
          entries = a.entries + b.entries)
      }
    }
  }


  final case class Diff(value: Long)

  object Diff {

    val Empty: Diff = Diff(0)

    def of(diffs: List[Diff]): Option[Diff] = {
      diffs
        .toNel
        .map { diffs => Diff(diffs.foldMap { _.value } / diffs.size) }
    }

    def of(prev: Offset, next: Offset): Option[Diff] = {
      of(
        prev = prev.value,
        next = next.value)
    }

    def of(prev: Long, next: Long): Option[Diff] = {
      if (prev < next) {
        Diff(next - prev).some
      } else {
        none
      }
    }

    implicit val commutativeMonoidDiff: CommutativeMonoid[Diff] = new CommutativeMonoid[Diff] {
      def empty = Empty
      def combine(a: Diff, b: Diff) = Diff(a.value + b.value)
    }
  }

  final case class Record(offset: Offset, data: Option[Record.Data])

  object Record {

    final case class Data(id: String, header: ActionHeader)
    def apply(id: String, offset: Offset, header: ActionHeader): Record = {
      apply(offset, Data(id, header).some)
    }
  }

  private final case class Entry(offset: Offset, headInfo: HeadInfo.NonEmpty)

  private object Entry {
    implicit val semigroupEntry: Semigroup[Entry] = {
      (a: Entry, b: Entry) => {
        Entry(
          headInfo = a.headInfo combine b.headInfo,
          offset = a.offset max b.offset)
      }
    }

    implicit val orderingEntry: Ordering[Entry] = Ordering.by { (a: Entry) => a.offset }(Offset.orderingOffset.reverse)
  }

  private final case class Entries(bounds: Bounds[Offset], values: Map[String, Entry])

  private object Entries {
    implicit val semigroupEntries: Semigroup[Entries] = {
      (a: Entries, b: Entries) => {
        Entries(
          values = a.values combine b.values,
          bounds = a.bounds combine b.bounds)
      }
    }

    implicit class EntriesOps(val self: Entries) extends AnyVal {
      def limit[F[_]: MonadThrow](maxSize: Int, dropUponLimit: Double): F[Option[Entries]] = {
        if (self.values.size <= maxSize) {
          self
            .some
            .pure[F]
        } else {
          val drop = (maxSize * dropUponLimit).toInt
          val take = (maxSize - drop).max(1)
          val values = self
            .values
            .toList
            .sortBy { case (_, entry) => entry }
            .take(take)
          val (_, entry) = values.minBy { case (_, entry) => entry.offset }
          Bounds
            .of[F](
              min = entry.offset,
              max = self.bounds.max)
            .map { bounds =>
              Entries
                .apply(bounds, values.toMap)
                .some
            }
        }
      }
    }
  }

  private final case class State(offset: Option[Offset], entries: Option[Entries])

  private object State {
    implicit class StateOps(val self: State) extends AnyVal {
      def ahead(offset: Offset): Boolean = {
        self
          .offset
          .exists { _ >= offset }
      }

      def result(id: String, offset: Offset): Either[Option[Entries], Result.Now] = {
        if (self.ahead(offset)) {
          Result
            .Now
            .ahead
            .asRight[Option[Entries]]
        } else {
          self
            .entries
            .fold {
              none[Entries].asLeft[Result.Now]
            } { entries =>
              if (offset >= entries.bounds.min) {
                if (offset <= entries.bounds.max) {
                  val headInfo = entries
                    .values
                    .get(id)
                    .map { _.headInfo }
                    .getOrElse { HeadInfo.empty }
                  Result
                    .Now
                    .value(headInfo)
                    .asRight[Option[Entries]]
                } else {
                  entries
                    .some
                    .asLeft[Result.Now]
                }
              } else {
                Result
                  .Now
                  .limited
                  .asRight[Option[Entries]]
              }
            }
        }
      }
    }
  }

  implicit class PartitionCacheOps[F[_]](val self: PartitionCache[F]) extends AnyVal {
    def add(record: Record, records: Record*): F[Option[Diff]] = {
      self.add(Nel.of(record, records: _*))
    }
  }

  private implicit class CacheOps[F[_], K, V](val self: Cache[F, K, V]) extends AnyVal {
    def foldMapPar1[A](f: V => F[A])(implicit F: Sync[F], commutativeMonoid: CommutativeMonoid[A]): F[A] = {
      self.foldMapPar {
        case (_, Right(a)) =>
          f(a)
        case (_, Left(a))  =>
          a
            .attempt
            .flatMap {
              case Right(a) => f(a)
              case Left(_)  => CommutativeMonoid[A].empty.pure[F]
            }
      }
    }
  }
}