package com.evolutiongaming.kafka.journal.replicator

import cats.data.NonEmptySet as Nes
import cats.effect.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.{Defer, Parallel}
import com.evolutiongaming.catshelper.ParallelHelper.*
import com.evolutiongaming.catshelper.{FromTry, LogOf}
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.Retry.implicits.*
import com.evolutiongaming.retry.{OnError, Sleep, Strategy}
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, RebalanceListener1}
import com.evolutiongaming.skafka.consumer.RebalanceCallback.syntax._
import com.evolutiongaming.skafka.{Partition, Topic, TopicPartition}

import scala.concurrent.duration.*

/**
 * [[DistributeJob]] is a service that distributes jobs across application nodes.
 * 
 * Distribution done with help of Apache Kafka consumers working within a single consumer group.
 * Each job expected to process subset of its data based on Kafka partitions assignment, API:
 * {{{
 * val job = (assignedPartitions: Map[Partition, Assigned]) => {
 *   assignedPartitions
 *     .collect { case (assigned, true) => assigned }
 *     .toList
 *     .map(_.value)
 *     .parTraverse_ { n: Int =>
 *        val jobSubset: Resource[F, Unit] = ??? // use `n` in calculating job subset to be done
 *         jobSubset
 *      }
 * }
 * }}}
 * 
 * In case of Kafka partitions rebalance (i.e. on application node restart), the service will
 * release jobs from unassigned partitions and re-allocate them to the new assigned partitions.
 * Please take into account that any job must be tolerant to execution of [[Resource]]'s release
 * action while it is still running. The service does not cancel or interrupt running jobs any other way 
 * than releasing corresponding [[Resource]]. 
 * 
 * Please note that Kafka topic used for job distribution must have at least as many partitions as the number of application nodes.
 */
trait DistributeJob[F[_]] {
  import DistributeJob.Assigned

  /**
    * Assigns a job to be distributed across application nodes.
    *
    * @param name job name, expected to be unique within the application. Otherwise [[DistributeJobError]] will be raised.
    * @param job job factory aimed to provide the job to process subset of its data based on Kafka partitions assignment.
    * @return a resource that will be released when the job is done or failed.
    */
  def apply(name: String)(job: Map[Partition, Assigned] => Option[Resource[F, Unit]]): Resource[F, Unit]

  /**
    * Assigns a job to one of the application nodes.
    *
    * @param name job name, expected to be unique within the application. Otherwise [[DistributeJobError]] will be raised.
    * @param job the job to be done.
    * @return a resource that will be released when the job is done or failed.
    */
  def single(name: String)(job: Resource[F, Unit]): Resource[F, Unit]
}

object DistributeJob {

  type Assigned = Boolean

  def apply[F[_]: Concurrent: Defer: Sleep: FromTry: LogOf: Parallel](
    groupId: String,
    topic: Topic,
    consumerConfig: ConsumerConfig,
    kafkaConsumerOf: KafkaConsumerOf[F],
  ): Resource[F, DistributeJob[F]] = {

    val consumerConfig1 = consumerConfig.copy(autoCommit = true, groupId = groupId.some, autoOffsetReset = AutoOffsetReset.Latest)

    type Job = Map[Partition, Assigned] => Option[Resource[F, Unit]]

    type Release = F[Unit]

    sealed abstract class S

    object S {
      def empty: S = Idle(Map.empty)

      sealed abstract class Allocated extends S

      final case class Idle(jobs: Map[String, Job]) extends Allocated

      final case class Active(partitions: Map[Partition, Assigned], jobs: Map[String, (Job, Release)]) extends Allocated

      final case object Released extends S
    }

    val unit = ().pure[F]

    for {
      log    <- LogOf[F].apply(DistributeJob.getClass).toResource
      random <- Random.State.fromClock[F]().toResource
      strategy = Strategy
        .exponential(100.millis)
        .jitter(random)
        .limit(1.hour)
        .resetAfter(5.minutes)
      onError   = OnError.fromLog(log)
      deferred <- Deferred[F, Either[Throwable, Unit]].toResource
      launched = (a: Either[Throwable, Unit]) =>
        deferred
          .complete(a)
          .void
          .handleError { _ => () }
      release = (jobs: Map[String, (Job, Release)]) => {
        Defer[F].defer {
          jobs.parFoldMap1 {
            case (name, (_, release)) =>
              release.handleErrorWith { a => log.error(s"release failed, job: $name, error: $a", a) }
          }
        }
      }
      ref <- Resource.make {
        Ref[F].of(S.empty)
      } { ref =>
        ref.modify {
          case _: S.Idle   => (S.Released, unit)
          case s: S.Active => (S.Released, release(s.jobs))
          case S.Released  => (S.Released, unit)
        }.flatten
      }
      modify = (f: S.Allocated => (S.Allocated, F[Unit])) => {
        ref
          .modify {
            case s: S.Allocated => f(s)
            case S.Released     => (S.Released, DistributeJobError("already released").raiseError[F, Unit])
          }
          .flatten
          .uncancelable
      }
      resource = for {
        consumer      <- kafkaConsumerOf[String, Unit](consumerConfig1)
        partitionsAll <- consumer.partitions(topic).toResource
        active = (
          deferred: Deferred[F, String => F[Unit]],
          jobs: Map[String, (Job, Release)],
          partitions: Map[Partition, Assigned],
        ) => {
          val jobs1 = jobs.map {
            case (name, (job, _)) =>
              val release = deferred.get.flatMap { _.apply(name) }
              (name, (job, release))
          }
          val effect = Defer[F].defer {
            for {
              releases <- jobs
                .toList
                .parTraverse {
                  case (name, (job, release)) =>
                    for {
                      _ <- release.handleErrorWith { a => log.error(s"release failed, job: $name, error: $a", a) }
                      a <- job(partitions) match {
                        case Some(job) =>
                          job
                            .allocated
                            .attempt
                            .flatMap {
                              case Right(((), release)) =>
                                (name, release).pure[F]
                              case Left(a) =>
                                log
                                  .error(s"allocate failed, job: $name, error: $a", a)
                                  .as((name, unit))
                            }
                        case None =>
                          (name, unit).pure[F]
                      }
                    } yield a
                }
              releases <- releases.toMap.pure[F]
              release   = (name: String) => releases.get(name).foldA
              result   <- deferred.complete(release)
            } yield result
          }
          (S.Active(partitions, jobs1), effect.void)
        }
        listener = new RebalanceListener1[F] {
          def onPartitionsAssigned(topicPartitions: Nes[TopicPartition]) = {
            val partitionsAssigned = topicPartitions.map { _.partition }
            val effect = for {
              d <- Deferred[F, String => F[Unit]]
              a <- modify {
                case s: S.Idle =>
                  val jobs = s.jobs.map { case (name, job) => (name, (job, unit)) }
                  val partitions = partitionsAll.map { partition =>
                    val assigned = partitionsAssigned.contains_(partition)
                    (partition, assigned)
                  }.toMap
                  active(d, jobs, partitions)
                case s: S.Active =>
                  if (partitionsAssigned.forall { partition => s.partitions.getOrElse(partition, false) }) {
                    (s, unit)
                  } else {
                    val partitions = s.partitions ++ partitionsAssigned.map { (_, true) }.toIterable
                    active(d, s.jobs, partitions)
                  }
              }
            } yield a
            effect.lift
          }

          def onPartitionsRevoked(topicPartitions: Nes[TopicPartition]) = {
            val partitionsRevoked = topicPartitions.map { _.partition }
            val effect = for {
              d <- Deferred[F, String => F[Unit]]
              a <- modify {
                case s: S.Idle => (s, unit)
                case s: S.Active =>
                  if (partitionsRevoked.forall { partition => !s.partitions.getOrElse(partition, false) }) {
                    (s, unit)
                  } else {
                    val partitions = s.partitions ++ partitionsRevoked.map { (_, false) }.toIterable
                    if (s.partitions === partitions) {
                      val effect = Defer[F].defer {
                        s
                          .jobs
                          .parFoldMap1 {
                            case (name, (_, release)) =>
                              release.handleErrorWith { a => log.error(s"release failed, job: $name, error: $a", a) }
                          }
                      }
                      val jobs = s.jobs.map { case (name, (job, _)) => (name, job) }
                      (S.Idle(jobs), effect)
                    } else {
                      active(d, s.jobs, s.partitions ++ partitions)
                    }
                  }
              }
            } yield a
            effect.lift
          }

          def onPartitionsLost(topicPartitions: Nes[TopicPartition]) = {
            onPartitionsRevoked(topicPartitions)
          }
        }
        _ <- consumer
          .subscribe(topic, listener)
          .toResource
        poll = consumer.poll(10.millis)
        _   <- poll.toResource
        _   <- launched(().asRight).toResource
      } yield {
        poll.foreverM[Unit]
      }
      _ <- resource
        .use(identity)
        .guaranteeCase {
          case Outcome.Canceled()   => launched(().asRight)
          case Outcome.Errored(a)   => launched(a.asLeft)
          case Outcome.Succeeded(_) => launched(().asRight)
        }
        .retry(strategy, onError)
        .background
      _ <- deferred
        .get
        .rethrow
        .toResource
    } yield {
      new DistributeJob[F] {

        def apply(name: String)(job: Map[Partition, Assigned] => Option[Resource[F, Unit]]) = {
          Resource.make {
            for {
              d <- Deferred[F, F[Unit]]
              a <- modify {
                case s: S.Idle =>
                  s.jobs.get(name) match {
                    case Some(_) => (s, DistributeJobError("duplicate job").raiseError[F, Unit])
                    case None    => (s.copy(jobs = s.jobs.updated(name, job)), unit)
                  }
                case s: S.Active =>
                  s.jobs.get(name) match {
                    case Some(_) =>
                      (s, DistributeJobError("duplicate job").raiseError[F, Unit])
                    case None =>
                      val (effect, release) = job(s.partitions) match {
                        case Some(job) =>
                          val effect = Defer[F].defer {
                            for {
                              a <- job
                                .allocated
                                .attempt
                              a <- a match {
                                case Right(((), a)) =>
                                  d.complete(a).void
                                case Left(a) =>
                                  for {
                                    _ <- d.complete(unit)
                                    _ <- ref.update {
                                      case s: S.Idle   => s.copy(jobs = s.jobs - name)
                                      case s: S.Active => s.copy(jobs = s.jobs - name)
                                      case S.Released  => S.Released
                                    }
                                    a <- a.raiseError[F, Unit]
                                  } yield a
                              }
                            } yield a
                          }
                          (effect, d.get.flatten)

                        case None => (unit, unit)
                      }
                      (s.copy(jobs = s.jobs.updated(name, (job, release))), effect)
                  }
              }
            } yield a
          } { _ =>
            modify {
              case s: S.Idle => (s.copy(jobs = s.jobs - name), unit)
              case s: S.Active =>
                s.jobs.get(name) match {
                  case Some((_, release)) => (s.copy(jobs = s.jobs - name), release)
                  case None               => (s, unit)
                }
            }
          }
        }

        def single(name: String)(job: Resource[F, Unit]) = {
          apply(name) { partitions =>
            if (partitions.getOrElse(Partition.min, false)) {
              job.some
            } else {
              none
            }
          }
        }
      }
    }
  }
}

final case class DistributeJobError(
  msg: String,
  cause: Option[Throwable] = None,
) extends RuntimeException(msg, cause.orNull)

object DistributeJobError {

  def apply(msg: String, cause: Throwable): DistributeJobError = DistributeJobError(msg, cause.some)
}
