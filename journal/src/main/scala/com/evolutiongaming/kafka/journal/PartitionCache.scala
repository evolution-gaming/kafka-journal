package com.evolutiongaming.kafka.journal

import cats.{Monad, MonadThrow, Semigroup}
import cats.data.{NonEmptyList => Nel}
import cats.effect.{Concurrent, Resource, Timer}
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.syntax.all._
import cats.kernel.CommutativeMonoid
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
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

  /** Creates [[PartitionCache]] using configured parameters.
    *
    * The parameters are, usually, configured in [[HeadCacheConfig]], but could
    * also be set directly, i.e. for unit testing purposes.
    *
    * @param maxSize
    *   Maximum number of journals to store in the cache.
    * @param dropUponLimit
    *   Proportion of number of journals to drop if `maxSize` is reached. Value
    *   outside of the range of `0.01` to `1.0` will be ignored. `0.01` means
    *   that 1% of journals will get dropped, and `1.0` means that 100% of
    *   journals will get dropped.
    * @param timeout
    *   Duration to wait in [[Result.Later]] returned by [[Partition#get]]
    *   if entry is not found in a cache.
    * @return
    *   Resource which will configure a [[PartitionCache]] with the passed
    *   parameters. Instance of `Resource[PartitionCache]` are, obviously,
    *   reusable and there is no need to call [[PartitionCache#of]] each time if
    *   parameters did not change.
    */
  def of[F[_]: Concurrent: Timer](
    maxSize: Int = 10000,
    dropUponLimit: Double = 0.1,
    timeout: FiniteDuration
  ): Resource[F, PartitionCache[F]] = {
    main(
      maxSize = maxSize.max(1),
      dropUponLimit = dropUponLimit.max(0.01).min(1.0),
      timeout = timeout)
  }

  /** Same as [[#of]], but without default parameters */
  private def main[F[_]: Concurrent: Timer](
    maxSize: Int,
    dropUponLimit: Double,
    timeout: FiniteDuration
  ): Resource[F, PartitionCache[F]] = {

    /** Listener waiting for latest [[HeadInfo]] to appear in the cache.
      *
      * When [[PartitionCache#get]] cannot find an actual entry for a given
      * journal, it sets up an expiring listener (or deferred value), which
      * returns an actual information if it gets into a cache in a timely manner,
      * or [[Result.Now.Timeout]] if the configured timeout expires.
      */
    final case class Listener(id: String, offset: Offset, deferred: Deferred[F, Either[Throwable, Result.Now]])

    type ListenerId = Int

    /** Actual state of [[PartitionCache]].
      *
      * @param listenerId
      * id to be use for next listener created
      * @param offset
      * The last _replicated_ offset seen by [[PartitionCache]] in Cassandra (or
      * another long term storage).
      * @param entries
      * Information about non-replicated events seen by [[PartitionCache]] in
      * Kafka.
      * @param listeners
      * Listener waiting for latest [[HeadInfo]] to appear in the cache.
      */
    final case class State(
      listenerId: ListenerId,
      offset: Option[Offset],
      entries: Option[Entries],
      listeners: Map[ListenerId, Listener]
    ) { self =>
      /** Checks if current state is ahead of Cassandra.
        *
        * @param offset
        * The current offset (offset of the marker).
        * @see
        * [[Result.Now.Ahead]] for more details.
        */
      def ahead(offset: Offset): Boolean = {
        self
          .offset
          .exists { _ >= offset }
      }
    }

    Resource
      .make {
        Ref[F].of(
          State
            .apply(
              listenerId = 0,
              offset = none,
              entries = none,
              listeners = Map.empty)
            .asRight[Throwable])
      } { ref =>
        0.tailRecM { count =>
          ref
            .access
            .flatMap {
              case (Right(state), set) =>
                set
                  .apply(ReleasedError.asLeft)
                  .flatMap {
                    case true  =>
                      state
                        .listeners
                        .values
                        .toList
                        .foldMapM { listener =>
                          listener
                            .deferred
                            .complete(ReleasedError.asLeft)
                            .handleError { _ => () }
                        }
                        .map { _.asRight[Int] }
                    case false =>
                      (count + 1)
                        .asLeft[Unit]
                        .pure[F]
                  }
              case _                   =>
                ()
                  .asRight[Int]
                  .pure[F]
            }
        }
      }
      .map { ref =>

        class Main
        new Main with PartitionCache[F] {

          def get(id: String, offset: Offset) = {
            0.tailRecM { count =>
              ref
                .access
                .flatMap {
                  case (Right(state), set) =>
                    val result = {
                      if (state.ahead(offset)) {
                        Result
                          .Now
                          .ahead
                          .some
                      } else {
                        state
                          .entries
                          .flatMap { _.result(id, offset) }
                      }
                    }
                    result
                      .map { result =>
                        result
                          .toResult[F]
                          .asRight[Int]
                          .pure[F]
                      }
                      .getOrElse {
                        Deferred
                          .apply[F, Either[Throwable, Result.Now]]
                          .flatMap { deferred =>
                            val listenerId = state.listenerId
                            set
                              .apply {
                                state
                                  .copy(
                                    listenerId = listenerId + 1,
                                    listeners = state
                                      .listeners
                                      .updated(
                                        listenerId,
                                        Listener(id, offset, deferred)))
                                  .asRight
                              }
                              .flatMap {
                                case true =>
                                  Timer[F]
                                    .sleep(timeout)
                                    .productR {
                                      deferred
                                        .complete(Result.Now.timeout(timeout).asRight)
                                        .productR {
                                          0.tailRecM { count =>
                                            ref
                                              .access
                                              .flatMap {
                                                case (Right(state), set) =>
                                                  set
                                                    .apply {
                                                      state
                                                        .copy(listeners = state.listeners - listenerId)
                                                        .asRight
                                                    }
                                                    .map {
                                                      case true  => ().asRight[Int]
                                                      case false => (count + 1).asLeft[Unit]
                                                    }
                                                case (Left(_), _)        =>
                                                  ()
                                                    .asRight[Int]
                                                    .pure[F]
                                              }
                                          }
                                        }
                                        .handleError { _ => () }
                                    }
                                    .start
                                    .as {
                                      val get = deferred
                                        .get
                                        .rethrow
                                      val result: Result[F] = state.entries match {
                                        case Some(_) => Result.behind(get)
                                        case None    => Result.empty(get)
                                      }
                                      result.asRight[Int]
                                    }

                                case false =>
                                  (count + 1)
                                    .asLeft[Result[F]]
                                    .pure[F]
                              }
                              .uncancelable
                          }
                      }

                  case (Left(error), _) =>
                    error.raiseError[F, Either[Int, Result[F]]]
                }
            }
          }

          def offset = {
            ref
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
              bounds  <- Bounds.of[F](
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
                (offset, _)   = values.maxBy { case (offset, _) => offset }
                info          = values.foldLeft(HeadInfo.empty) { case (info, (offset, data)) => info(data.header, offset) }
                entry        <- info match {
                  case HeadInfo.Empty       => none[Entry]
                  case a: HeadInfo.NonEmpty => Entry(offset = offset, a).some
                }
              } yield {
                (id, entry)
              }
              entries  = Entries(bounds = bounds, values = values.toMap)

              result  <- 0.tailRecM { counter =>
                ref
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
                            val listeners = state.listeners
                            val (listeners1, effect) = listeners.foldLeft((listeners, ().pure[F])) {
                              case ((listeners, effect), (listenerId, listener)) =>
                                entriesNotLimited
                                  .result(listener.id, listener.offset)
                                  .fold {
                                    (listeners, effect)
                                  } { result =>
                                    (
                                      listeners - listenerId,
                                      effect.productR {
                                        listener
                                          .deferred
                                          .complete(result.asRight)
                                          .handleError { _ => () }
                                      }
                                    )
                                  }
                            }
                            set
                              .apply {
                                state
                                  .copy(
                                    listeners = listeners1,
                                    entries = entries.some)
                                  .asRight
                              }
                              .flatMap {
                                case true  =>
                                  effect.as {
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
                              .uncancelable
                          }
                      }
                  }
              }
            } yield result
          }

          def remove(offset: Offset) = {
            0.tailRecM { counter =>
              ref
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
                            val listeners = state.listeners
                            val (listeners1, effect) = listeners.foldLeft((listeners, ().pure[F])) {
                              case ((listeners, effect), (listenerId, listener)) =>
                                if (offset >= listener.offset) {
                                  (
                                    listeners - listenerId,
                                    effect.productR {
                                      listener
                                        .deferred
                                        .complete {
                                          Result
                                            .Now
                                            .ahead
                                            .asRight
                                        }
                                        .void
                                    }
                                  )
                                } else {
                                  (listeners, effect)
                                }
                            }
                            set
                              .apply {
                                state
                                  .copy(
                                    offset = offset.some,
                                    entries = entries,
                                    listeners = listeners1)
                                  .asRight
                              }
                              .flatMap {
                                case true  =>
                                  effect.as {
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
                              .uncancelable
                          }
                      }
                    }
                }
            }
          }

          def meters = {
            ref
              .get
              .map { state =>
                state.foldMap { state =>
                  Meters(
                    listeners = state.listeners.size,
                    entries = state.entries.foldMap { _.values.size })
                }
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

  /** Runtime parameters that could be used for metrics.
    *
    * @param listeners
    *   Number of listeners waiting after [[PartitionCache#get]] call. Too many
    *   of them might mean that cache is not being loaded fast enough.
    * @param entries
    *   Number of distinct journals stored in a cache. If it is too close to
    *   maximum configured number, the cache might not work efficiently.
    */
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
      def limit[F[_]: MonadThrow](maxSize: Int, dropUponLimit: Double): F[Entries] = {
        if (self.values.size <= maxSize) {
          self.pure[F]
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
              Entries(bounds, values.toMap)
            }
        }
      }

      def result(id: String, offset: Offset): Option[Result.Now] = {

        def entry = {
          self
            .values
            .get(id)
        }

        if (offset >= self.bounds.min) {
          if (offset <= self.bounds.max) {
            Result
              .Now
              .value {
                entry
                  .map { _.headInfo }
                  .getOrElse { HeadInfo.empty }
              }
              .some
          } else {
            none[Result.Now]
          }
        } else {
          entry
            .fold {
              Result
                .Now
                .limited
            } { entry =>
              Result
                .Now
                .value(entry.headInfo)
            }
            .some
        }
      }
    }
  }

  implicit class PartitionCacheOps[F[_]](val self: PartitionCache[F]) extends AnyVal {
    def add(record: Record, records: Record*): F[Option[Diff]] = {
      self.add(Nel.of(record, records: _*))
    }
  }
}