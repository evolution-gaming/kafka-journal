package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import cats.effect._
import cats.effect.syntax.all._
import cats.kernel.CommutativeMonoid
import cats.syntax.all._
import cats.{Monad, MonadThrow, Semigroup}
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.skafka.Offset

import scala.concurrent.duration.FiniteDuration

/** Maintains an information about non-replicated Kafka records in a partition.
  *
  * The class itself does not read Kafka or poll Cassandra (or other long term
  * storage), it relies on the information incoming through
  * [[PartitionCache#add]] (for Kafka updates), and [[PartitionCache#remove]]
  * (for Cassandra updates).
  */
trait PartitionCache[F[_]] {

  /** Get the information about a state of a journal stored in this partition.
    *
    * @param id
    *   Journal id
    * @param offset
    *   Current [[Offset]], i.e. maximum offset where Kafka records related to a
    *   journal are located. The usual way to get such an offset is to write a
    *   "marker" record to Kafka patition and use the offset of the marker as a
    *   current one.
    *
    * @return
    *   [[PartitionCache.Result]] with either the current state or indication of
    *   a reason why such state is not present in a cache.
    *
    * @see
    *   [[PartitionCache.Result]] for more details on possible results.
    */
  def get(id: String, offset: Offset): F[PartitionCache.Result[F]]

  /** Last offset seen by [[PartitionCache]] either in Kafka or Cassandra.
    *
    * Such a value is useful, because there is no point to read Kafka for new
    * non-replicated events earlier than this offset.
    *
    * I.e. if we seen it in Cassandra, it means the events are already
    * replicated, and if we seen in Kafka, it means we already handled them in
    * our [[#add]] method.
    *
    * @return
    *   Last [[Offset]] seen by [[PartitionCache]], or `None`, if cache is
    *   empty.
    */
  def offset: F[Option[Offset]]

  /** Inform this cache about a batch of, potentially, non-replicated events.
    *
    * The method is intended to be called after the information is received from
    * Kafka. It is possible that part or all of these events are already
    * replicated.
    *
    * @param records
    *   Metainformation of the incoming records.
    * @return
    *   Number of additional records marked as non-replicated after the method
    *   was called. `None` if no new records were counted as non-replicated or
    *   if this is the first call on an empty cache.
    */
  def add(records: Nel[PartitionCache.Record]): F[Option[PartitionCache.Diff]]

  /** Inform this cache about the latest offset replicated to Cassandra.
    *
    * @param offset
    *   All events with offset less or equal to the parameter are now replicated
    *   to Cassandra (or other long term storage).
    * @return
    *   Number of additional records marked as replicated after the method was
    *   called. `None` if such offset or later was already removed or not
    *   present in the cache in the first place.
    */
  def remove(offset: Offset): F[Option[PartitionCache.Diff]]

  /** Runtime parameters that could be used for metrics.
    *
    * The call might be relatively expensive if there are a lot of
    * non-replicated journals, so it might be worth to call it outside of the
    * large loops. I.e. call it once a minute to update the metrics.
    *
    * @see [[PartitionCache.Meters]] for more details.
    */
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
  def of[F[_]: Async](
    maxSize: Int          = 10000,
    dropUponLimit: Double = 0.1,
    timeout: FiniteDuration,
  ): Resource[F, PartitionCache[F]] = {
    main(maxSize = maxSize.max(1), dropUponLimit = dropUponLimit.max(0.01).min(1.0), timeout = timeout)
  }

  /** Same as [[#of]], but without default parameters */
  private def main[F[_]: Async](
    maxSize: Int,
    dropUponLimit: Double,
    timeout: FiniteDuration,
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
      listeners: Map[ListenerId, Listener],
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
            .apply(listenerId = 0, offset = none, entries = none, listeners = Map.empty)
            .asRight[Throwable],
        )
      } { ref =>
        0.tailRecM { count =>
          ref
            .access
            .flatMap {
              case (Right(state), set) =>
                set
                  .apply(ReleasedError.asLeft)
                  .flatMap {
                    case true =>
                      state
                        .listeners
                        .values
                        .toList
                        .foldMapM { listener =>
                          listener
                            .deferred
                            .complete(ReleasedError.asLeft)
                            .void
                        }
                        .map { _.asRight[Int] }
                    case false =>
                      (count + 1)
                        .asLeft[Unit]
                        .pure[F]
                  }
              case _ =>
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
                                      .updated(listenerId, Listener(id, offset, deferred)),
                                  )
                                  .asRight
                              }
                              .flatMap {
                                case true =>
                                  Temporal[F]
                                    .sleep(timeout)
                                    .productR {
                                      deferred
                                        .complete(Result.Now.timeout(timeout).asRight)
                                        .flatMap {
                                          case true =>
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
                                                  case (Left(_), _) =>
                                                    ()
                                                      .asRight[Int]
                                                      .pure[F]
                                                }
                                            }
                                          case false =>
                                            ().pure[F]
                                        }
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
              bounds <- Bounds.of[F](
                min = records.minimumBy { _.offset }.offset,
                max = records.maximumBy { _.offset }.offset,
              )
              values = for {
                (id, values) <- records
                  .toList
                  .collect { case Record(offset, Some(data)) => (offset, data) }
                  .groupBy { case (_, record) => record.id }
                (offset, _) = values.maxBy { case (offset, _) => offset }
                info        = values.foldLeft(HeadInfo.empty) { case (info, (offset, data)) => info(data.header, offset) }
                entry <- info match {
                  case HeadInfo.Empty       => none[Entry]
                  case a: HeadInfo.NonEmpty => Entry(offset = offset, a).some
                }
              } yield {
                (id, entry)
              }
              entries = Entries(bounds = bounds, values = values.toMap)

              result <- 0.tailRecM { counter =>
                ref
                  .access
                  .flatMap {
                    case (state, set) =>
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
                                            .void
                                        },
                                      )
                                    }
                              }
                              set
                                .apply {
                                  state
                                    .copy(listeners = listeners1, entries = entries.some)
                                    .asRight
                                }
                                .flatMap {
                                  case true =>
                                    effect.as {
                                      state
                                        .entries
                                        .flatMap { entries =>
                                          Diff.of(prev = entries.bounds.max, next = bounds.max)
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
                .flatMap {
                  case (state, set) =>
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
                                      },
                                    )
                                  } else {
                                    (listeners, effect)
                                  }
                              }
                              set
                                .apply {
                                  state
                                    .copy(offset = offset.some, entries = entries, listeners = listeners1)
                                    .asRight
                                }
                                .flatMap {
                                  case true =>
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
                  Meters(listeners = state.listeners.size, entries = state.entries.foldMap { _.values.size })
                }
              }
          }
        }
      }
  }

  /** State of the non-replicated journal head comparing to a given offset. */
  sealed trait Result[+F[_]]

  object Result {

    /** [[PartitionCache]] has seen given offset in Kafka, but not Cassandra.
      *
      * Same as [[Now.Value]], but returns [[Result]].
      *
      * @see [[Now.Value]] for more details.
      */
    def value[F[_]](value: HeadInfo): Result[F] = Now.value(value)

    /** [[PartitionCache]] has seen given offset in Cassandra.
      *
      * Same as [[Now.Ahead]], but returns [[Result]].
      *
      * @see [[Now.Ahead]] for more details.
      */
    def ahead[F[_]]: Result[F] = Now.ahead

    /** [[HeadInfo]] was dropped because maximum cache size was reached.
      *
      * Same as [[Now.Limited]], but returns [[Result]].
      *
      * @see [[Now.Limited]] for more details.
      */
    def limited[F[_]]: Result[F] = Now.limited

    /** The timeout occured while waiting for the [[HeadInfo]] value to load.
      *
      * Same as [[Now.Timeout]], but returns [[Result]].
      *
      * @see [[Now.Timeout]] for more details.
      */
    def timeout[F[_]](duration: FiniteDuration): Result[F] = Now.timeout(duration)

    /** The cache was behind Kafka when [[PartitionCache#get]] got called.
      *
      * Same as [[Later.Behind]], but returns [[Result]]
      */
    def behind[F[_]](value: F[Now]): Result[F] = Later.behind(value)

    /** The cache was empty when [[PartitionCache#get]] got called.
      *
      * Same as [[Later.Empty]], but returns [[Result]]
      */
    def empty[F[_]](value: F[Now]): Result[F] = Later.empty(value)

    /** [[PartitionCache]] already seen such [[Offset]] in Kafka or Cassandra.
      *
      * In other words, it means that this offset was either already replicated,
      * or had the latest [[HeadInfo]] information inside of [[PartitionCache]]
      * when [[PartitionCache#get]] was called.
      */
    sealed trait Now extends Result[Nothing]

    object Now {

      /** [[PartitionCache]] has seen given offset in Kafka, but not Cassandra.
        *
        * Same as [[Now.Value]], but returns [[Now]].
        *
        * @see [[Now.Value]] for more details.
        */
      def value(value: HeadInfo): Now = Value(value)

      /** [[PartitionCache]] has seen given offset in Cassandra.
        *
        * Same as [[Now.Ahead]], but returns [[Now]].
        */
      def ahead: Now = Ahead

      /** [[HeadInfo]] was dropped because maximum cache size was reached.
        *
        * Same as [[Now.Limited]], but returns [[Now]].
        *
        * @see [[Now.Limited]] for more details.
        */
      def limited: Now = Limited

      /** The timeout occured while waiting for the [[HeadInfo]] value to load.
        *
        * Same as [[Now.Timeout]], but returns [[Now]].
        *
        * @see [[Now.Timeout]] for more details.
        */
      def timeout(duration: FiniteDuration): Now = Timeout(duration)

      /** [[PartitionCache]] has seen given offset in Kafka, but not Cassandra.
        *
        * In other words, the journal is not fully replicated, but we have
        * [[HeadInfo]] in cache to be used for optimization purposes.
        *
        * @param value Actual [[HeadInfo]] for the given journal.
        */
      final case class Value(value: HeadInfo) extends Now

      /** [[PartitionCache]] has seen given offset in Cassandra.
        *
        * In other words, the journal is already fully replicated to a long term
        * storage.
        */
      final case object Ahead extends Now

      /** [[HeadInfo]] was dropped because maximum cache size was reached.
        *
        * In other words, [[PartitionCache]] has seen given offset in Kafka, but
        * we could not return a [[HeadInfo]] value to be used, and it has to be
        * calcuated again.
        */
      final case object Limited extends Now

      /** The timeout occured while waiting for the [[HeadInfo]] value to load.
        *
        * When [[PartitionCache#get]] is called and [[HeadInfo]] is not yet
        * available in the cache, the returned `F[Result]` will contain
        * [[Result.Later]], with value, which will try to wait until either
        * [[PartitionCache#add]] or [[PartitionCache#remove]] will bring the
        * required information into [[PartitionCache]].
        *
        * This avoids reading Kafka in parallel as, usually, the consumer
        * calling these methods on [[PartitionCache]] is already processing
        * these records.
        *
        * Saying that, if consumer is too slow (during Kafka rebalancing?), and
        * the information does not quickly replicate to Cassandra either, then
        * we want to report a timeout allowing the caller to handle the
        * situation by itself.
        *
        * @param duration
        *   The value of a timeout exceeded while waiting for a value to appear.
        *   In future it may become an actual time passed since
        *   [[PartitionCache#get]] call.
        */
      final case class Timeout(duration: FiniteDuration) extends Now

      implicit class NowOps(val self: Now) extends AnyVal {

        /** Widens [[Now]] to [[Result]].
          *
          * This might be, potentially, more performant than calling
          * `pure[F].widen[Result]`.
          */
        def toResult[F[_]]: Result[F] = self
      }
    }

    /** [[PartitionCache]] did not already see [[Offset]] in Kafka or Cassandra.
      *
      * When [[PartitionCache#get]] was called, the offset was not seen by
      * [[PartitionCache]] yet.
      *
      * The caller may still try to get [[HeadInfo]] without calculating it
      * themselves if [[PartitionCache]] gets the information, before configured
      * timeout kicks in, by calling [[Later#value]].
      *
      * @see
      *   [[Now.Timeout]] on more details of how this timeout works and why it
      *   is required.
      */
    sealed trait Later[F[_]] extends Result[F]

    object Later {

      /** The cache was behind Kafka when [[PartitionCache#get]] got called.
        *
        * Same as [[Behind]], but returns [[Result]]
        */
      def behind[F[_]](value: F[Now]): Result[F] = Behind(value)

      /** The cache was empty when [[PartitionCache#get]] got called.
        *
        * Same as [[Empty]], but returns [[Result]]
        */
      def empty[F[_]](value: F[Now]): Result[F] = Empty(value)

      /** The cache was behind Kafka when [[PartitionCache#get]] got called.
        *
        * It was also behind Casssandra or [[Now.Ahead]] would have returned.
        *
        * The caller may try to wait for an actual [[HeadInfo]] by calling
        * [[Empty#value]].
        *
        * @param value
        *   Placeholder for deferred entry.
        * @see
        *   [[Later]] for more details.
        */
      final case class Behind[F[_]](value: F[Now]) extends Later[F]

      /** The cache was empty when [[PartitionCache#get]] got called.
        *
        * The caller may try to wait for it to get filled by calling
        * [[Empty#value]].
        *
        * @param value
        *   Placeholder for deferred entry.
        * @see
        *   [[Later]] for more details.
        */
      final case class Empty[F[_]](value: F[Now]) extends Later[F]

      implicit class LaterOps[F[_]](val self: Later[F]) extends AnyVal {

        /** Placholder for deferred entry */
        def value: F[Now] = self match {
          case Behind(a) => a
          case Empty(a)  => a
        }

      }
    }

    implicit class ResultOps[F[_]](val self: Result[F]) extends AnyVal {

      /** Converts both [[Now]] and [[Later]] to [[Now]].
        *
        * Roughly speaking, the only case when a caller may care if [[Now]] was
        * immediately available or had to wait a bit for be loaded is metrics.
        *
        * In other cases, it might be fine to treat them as the same result.
        *
        * @see [[Result.Now.Timeout]] for more details.
        */
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
        Meters(listeners = a.listeners + b.listeners, entries = a.entries + b.entries)
      }
    }
  }

  /** Difference between two numerical values.
    *
    * Essentially it is just a wrapped [[Long]] value with a smart constructor
    * and, therefore some guaranteed properties, if constructor is used, i.e.
    * value being non-negative.
    *
    * The main benefit of this class is to avoid mix up with other numerical
    * values and to ensure correct calculation from the two input numbers.
    *
    * Example:
    * {{{
    * scala> import cats.syntax.all._
    * scala> import com.evolutiongaming.kafka.journal.PartitionCache.Diff
    *
    * scala> Diff.of(10, 20)
    * val res0: Option[Diff] = Some(Diff(10))
    *
    * scala> Diff.of(20, 10)
    * val res1: Option[Diff] = None
    *
    * scala> Diff.of(10, 10)
    * val res2: Option[Diff] = None
    *
    * scala> Diff(10).combine(Diff(20))
    * val res3: Diff = Diff(30)
    * }}}
    *
    * @param value
    *   Actual difference between the two numbers.
    */
  final case class Diff(value: Long)

  object Diff {

    /** Empty [[Diff]] value.
      *
      * It is used for [[CommutativeMonoid]] definition (i.e. to allow to
      * combine [[Diff]] values) and cannot be constructed using a smart
      * constructor.
      */
    val Empty: Diff = Diff(0)

    /** Calculate the difference between two offsets.
      *
      * @param prev
      *   Smaller (or older) offset.
      * @param next
      *   Larger (or newer) offset.
      * @return
      *   Difference between the offsets, or `None` if `prev` is larger or equal
      *   to `next`.
      */
    def of(prev: Offset, next: Offset): Option[Diff] = {
      of(prev = prev.value, next = next.value)
    }

    /** Calculate the difference between two numbers.
      *
      * @param prev
      *   Smaller number.
      * @param next
      *   Larger number.
      * @return
      *   Difference between the offsets, or `None` if `prev` is larger or equal
      *   to `next`.
      */
    def of(prev: Long, next: Long): Option[Diff] = {
      if (prev < next) {
        Diff(next - prev).some
      } else {
        none
      }
    }

    implicit val commutativeMonoidDiff: CommutativeMonoid[Diff] = new CommutativeMonoid[Diff] {
      def empty                     = Empty
      def combine(a: Diff, b: Diff) = Diff(a.value + b.value)
    }
  }

  /** Metainformation of, potentially, non-replicated Kafka record.
    *
    * The `data` field might empty if Kafka record does not contain header with
    * [[ActionHeader#key]]. Such records will be silently ignored by
    * [[PartitionCache#add]] method.
    *
    * @param offset
    *   [[Offset]] of the record.
    * @param data
    *   Actual metainformation including journal identifier and the purpose of
    *   the record, i.e. if it is append, delete etc., but not including an
    *   event payload.
    */
  final case class Record(offset: Offset, data: Option[Record.Data])

  object Record {

    /** Metainformation of a single Kafka record.
      *
      * @param id
      *   Journal identifier.
      * @param header
      *   Metainformation, including the purpose such as if this record appended
      *   events, deleted them or purged the journal.
      */
    final case class Data(id: String, header: ActionHeader)

    /** Convenience constructor, for records where `data` is strictly defined.
      *
      * At the moment of writing it was used in tests only.
      *
      * @see [[Record]] for more details.
      */
    def apply(id: String, offset: Offset, header: ActionHeader): Record = {
      apply(offset, Data(id, header).some)
    }
  }

  /** Cache entry for a single journal (i.e. single journal id).
    *
    * @param offset
    *   [[Offset]] of _last_ (i.e. newest) non-replicated record related to this
    *   journal, which was also seen by [[PartitionCache]].
    * @param headInfo
    *   [[HeadInfo]] of this journal.
    */
  private final case class Entry(offset: Offset, headInfo: HeadInfo.NonEmpty)

  private object Entry {
    implicit val semigroupEntry: Semigroup[Entry] = { (a: Entry, b: Entry) =>
      {
        Entry(headInfo = a.headInfo combine b.headInfo, offset = a.offset max b.offset)
      }
    }

    implicit val orderingEntry: Ordering[Entry] = Ordering.by { (a: Entry) => a.offset }(Offset.orderingOffset.reverse)
  }

  /** Entries for all journals related to one partition.
    *
    * @param bounds
    *   Part of the Kafka topic partition containing non-replicated events.
    *   Corresponds to a first non-replicated offset and the last Kafka offset
    *   seen by [[PartitionCache]].
    * @param values
    *   Journal specific entries (i.e. the key is journal id). All offsets
    *   stored there are meant to be within `bounds` interval.
    */
  private final case class Entries(bounds: Bounds[Offset], values: Map[String, Entry])

  private object Entries {
    implicit val semigroupEntries: Semigroup[Entries] = { (a: Entries, b: Entries) =>
      {
        Entries(values = a.values combine b.values, bounds = a.bounds combine b.bounds)
      }
    }

    implicit class EntriesOps(val self: Entries) extends AnyVal {

      /** Drops proportion of entries if number is higher than `maxSize`.
        *
        * @see [[PartitionCache#of]] for details on the meaning of parameters.
        */
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
            .of[F](min = entry.offset, max = self.bounds.max)
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
          entry.fold {
            Result
              .Now
              .limited
          } { entry =>
            Result
              .Now
              .value(entry.headInfo)
          }.some
        }
      }
    }
  }

  implicit class PartitionCacheOps[F[_]](val self: PartitionCache[F]) extends AnyVal {

    /** Same as [[PartitionCache#add]], but makes it a bit less verbose */
    def add(record: Record, records: Record*): F[Option[Diff]] = {
      self.add(Nel.of(record, records: _*))
    }
  }
}
