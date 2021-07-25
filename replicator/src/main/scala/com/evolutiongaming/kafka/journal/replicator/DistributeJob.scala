package com.evolutiongaming.kafka.journal.replicator

import cats.Parallel
import cats.data.{NonEmptySet => Nes}
import cats.effect.syntax.all._
import cats.effect.{Concurrent, ExitCase, Resource, Sync}
import cats.syntax.all._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{FromTry, LogOf}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.Retry.implicits._
import com.evolutiongaming.retry.{OnError, Strategy}
import com.evolutiongaming.skafka.{Partition, Topic, TopicPartition}
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, RebalanceListener}

import scala.concurrent.duration._
import cats.effect.{ Deferred, Ref, Temporal }

trait DistributeJob[F[_]] {
  import DistributeJob.Assigned

  def apply(name: String)(job: Map[Partition, Assigned] => Option[Resource[F, Unit]]): Resource[F, Unit]

  def single(name: String)(job: Resource[F, Unit]): Resource[F, Unit]
}

object DistributeJob {

  type Assigned = Boolean

  def apply[F[_]: Concurrent: FromTry: LogOf: Timer: Parallel](
    groupId: String,
    topic: Topic,
    consumerConfig: ConsumerConfig,
    kafkaConsumerOf: KafkaConsumerOf[F]
  ): Resource[F, DistributeJob[F]] = {

    val consumerConfig1 = consumerConfig.copy(
      autoCommit = true,
      groupId = groupId.some,
      autoOffsetReset = AutoOffsetReset.Latest)

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
      log <- LogOf[F].apply(DistributeJob.getClass).toResource
      random <- Random.State.fromClock[F]().toResource
      strategy = Strategy
        .exponential(100.millis)
        .jitter(random)
        .limit(1.hour)
        .resetAfter(5.minutes)
      onError = OnError.fromLog(log)
      deferred <- Deferred.tryable[F, Either[Throwable, Unit]].toResource
      launched = (a: Either[Throwable, Unit]) => deferred
        .complete(a)
        .handleError { _ => () }
      release = (jobs: Map[String, (Job, Release)]) => {
        Sync[F].defer {
          jobs
            .toList
            .parFoldMapA { case (name, (_, release)) =>
              release.handleErrorWith { a => log.error(s"release failed, job: $name, error: $a", a) }
            }
        }
      }
      ref <- Resource.make {
        Ref[F].of(S.empty)
      } { ref =>
        ref
          .modify {
            case _: S.Idle   => (S.Released, unit)
            case s: S.Active => (S.Released, release(s.jobs))
            case S.Released  => (S.Released, unit)
          }
          .flatten
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
        consumer <- kafkaConsumerOf[String, Unit](consumerConfig1)
        partitionsAll <- consumer.partitions(topic).toResource
        active = (deferred: Deferred[F, String => F[Unit]], jobs: Map[String, (Job, Release)], partitions: Map[Partition, Assigned]) => {
          val jobs1 = jobs.map { case (name, (job, _)) =>
            val release = deferred.get.flatMap { _.apply(name) }
            (name, (job, release))
          }
          val effect = Sync[F].defer {
            for {
              releases <- jobs
                .toList
                .parTraverse { case (name, (job, release)) =>
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
                            case Left(a)              =>
                              log
                                .error(s"allocate failed, job: $name, error: $a", a)
                                .as((name, unit))
                          }
                      case None      =>
                        (name, unit).pure[F]
                    }
                  } yield a
                }
              releases <- releases.toMap.pure[F]
              release   = (name: String) => releases.get(name).foldA
              result   <- deferred.complete(release)
            } yield result
          }
          (S.Active(partitions, jobs1), effect)
        }
        listener = new RebalanceListener[F] {
          def onPartitionsAssigned(topicPartitions: Nes[TopicPartition]) = {
            val partitionsAssigned = topicPartitions.map { _.partition }
            for {
              d <- Deferred[F, String => F[Unit]]
              a <- modify {
                case s: S.Idle   =>
                  val jobs = s.jobs.map { case (name, job) => (name, (job, unit)) }
                  val partitions = partitionsAll
                    .map { partition =>
                      val assigned = partitionsAssigned.contains_(partition)
                      (partition, assigned)
                    }
                    .toMap
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
          }

          def onPartitionsRevoked(topicPartitions: Nes[TopicPartition]) = {
            val partitionsRevoked = topicPartitions.map { _.partition }
            for {
              d <- Deferred[F, String => F[Unit]]
              a <- modify {
                case s: S.Idle   => (s, unit)
                case s: S.Active =>
                  if (partitionsRevoked.forall { partition => !s.partitions.getOrElse(partition, false) }) {
                    (s, unit)
                  } else {
                    val partitions = s.partitions ++ partitionsRevoked.map { (_, false) }.toIterable
                    if (s.partitions === partitions) {
                      val effect = Sync[F].defer {
                        s
                          .jobs
                          .toList
                          .parFoldMapA { case (name, (_, release)) =>
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
          }

          def onPartitionsLost(topicPartitions: Nes[TopicPartition]) = {
            onPartitionsRevoked(topicPartitions)
          }
        }
        _    <- consumer
          .subscribe(topic, listener.some)
          .toResource
        poll  = consumer.poll(10.millis)
        _    <- poll.toResource
        _    <- launched(().asRight).toResource
      } yield {
        poll.foreverM[Unit]
      }
      _ <- resource
        .use(identity)
        .guaranteeCase {
          case ExitCase.Canceled  => launched(().asRight)
          case ExitCase.Error(a)  => launched(a.asLeft)
          case ExitCase.Completed => launched(().asRight)
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
                case s: S.Idle   =>
                  s.jobs.get(name) match {
                    case Some(_) => (s, DistributeJobError("duplicate job").raiseError[F, Unit])
                    case None    => (s.copy(jobs = s.jobs.updated(name, job)), unit)
                  }
                case s: S.Active =>
                  s.jobs.get(name) match {
                    case Some(_) =>
                      (s, DistributeJobError("duplicate job").raiseError[F, Unit])
                    case None    =>
                      val (effect, release) = job(s.partitions) match {
                        case Some(job) =>
                          val effect = Sync[F].defer {
                            for {
                              a <- job
                                .allocated
                                .attempt
                              a <- a match {
                                case Right(((), a)) =>
                                  d.complete(a)
                                case Left(a)        =>
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
              case s: S.Idle   => (s.copy(jobs = s.jobs - name), unit)
              case s: S.Active => s.jobs.get(name) match {
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
  cause: Option[Throwable] = None
) extends RuntimeException(msg, cause.orNull)

object DistributeJobError {

  def apply(msg: String, cause: Throwable): DistributeJobError = DistributeJobError(msg, cause.some)
}


