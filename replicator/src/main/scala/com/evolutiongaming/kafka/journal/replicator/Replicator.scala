package com.evolutiongaming.kafka.journal.replicator


import akka.actor.ActorSystem
import cats.FlatMap
import cats.effect._
import cats.implicits._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.ReplicatedJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.ReplicatedCassandra
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.kafka.journal.util.ClockHelper._
import com.evolutiongaming.kafka.journal.util._
import com.evolutiongaming.scassandra.{CreateCluster, Session}
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{Topic, Bytes => _}

import scala.concurrent.{ExecutionContext, Future}

// TODO TEST
trait Replicator[F[_]] {

  def done: F[Boolean]

  def close: F[Unit]
}

object Replicator {

  def of[F[_] : Concurrent : Timer : Par : FromFuture : ToFuture](
    system: ActorSystem,
    metrics: Metrics[F] = Metrics.empty[F]): F[Replicator[F]] = {

    val config = {
      val config = system.settings.config.getConfig("evolutiongaming.kafka-journal.replicator")
      ReplicatorConfig(config)
    }

    for {
      cassandra  <- Sync[F].delay { CreateCluster(config.cassandra.client) }
      ecBlocking <- Sync[F].delay { system.dispatchers.lookup(config.blockingDispatcher) }
      session    <- FromFuture[F].apply { cassandra.connect() }
      replicator <- {
        implicit val session1 = session
        of1[F](config, ecBlocking, metrics)
      }
    } yield {
      new Replicator[F] {

        def done = {
          replicator.done
        }

        def close = {
          for {
            _ <- replicator.close
            _ <- FromFuture[F].apply { cassandra.close() }
          } yield {}
        }
      }
    }
  }

  def of1[F[_] : Concurrent : Timer : Par : FromFuture : ToFuture](
    config: ReplicatorConfig,
    ecBlocking: ExecutionContext,
    metrics: Metrics[F])(implicit
    session: Session): F[Replicator[F]] = {

    implicit val clock = TimerOf[F].clock

    val journal = {
      for {
        journal <- ReplicatedCassandra.of[F](config.cassandra)
        log     <- Log.of[F](ReplicatedCassandra.getClass)
      } yield {
        val logging = ReplicatedJournal[F](journal, log)
        metrics.journal.fold(logging) { ReplicatedJournal(logging, _) }
      }
    }

    for {
      journal <- journal
      replicator <- {
        implicit val journal1 = journal
        of2(config, ecBlocking, metrics)
      }
    } yield {
      replicator
    }
  }

  def of2[F[_] : Concurrent : Timer : Par : FromFuture : ReplicatedJournal](
    config: ReplicatorConfig,
    ecBlocking: ExecutionContext,
    metrics: Metrics[F]): F[Replicator[F]] = {

    val consumerOf = (config: ConsumerConfig) => {
      for {
        consumer <- Sync[F].delay { skafka.consumer.Consumer[Id, Bytes](config, ecBlocking) }
      } yield {
        metrics.consumer.fold(consumer) { skafka.consumer.Consumer(consumer, _) }
      }
    }

    val consumerOf1 = consumerOf.andThen { consumer =>
      for {
        consumer <- consumer
      } yield {
        Consumer[F](consumer)
      }
    }

    val createReplicator = (topic: Topic) => {
      val prefix = config.consumer.groupId getOrElse "journal-replicator"
      val groupId = s"$prefix-$topic"
      val consumerConfig = config.consumer.copy(
        groupId = Some(groupId),
        autoOffsetReset = AutoOffsetReset.Earliest,
        autoCommit = false)

      val c = for {
        consumer <- consumerOf(consumerConfig)
      } yield {
        TopicReplicator.Consumer[F](consumer, config.pollTimeout)
      }

      implicit val metrics1 = metrics.replicator.fold(TopicReplicator.Metrics.empty[F]) { _.apply(topic) }

      TopicReplicator.of[F](topic = topic, consumer = c)
    }

    of(config, consumerOf1, createReplicator)
  }

  def of[F[_] : Concurrent : Timer : Par : FromFuture](
    config: ReplicatorConfig,
    consumerOf: ConsumerConfig => F[Consumer[F]],
    topicReplicatorOf: Topic => F[TopicReplicator[F]]): F[Replicator[F]] = {

    implicit val clock: Clock[F] = TimerOf[F].clock

    sealed trait State

    object State {

      def closed: State = Closed

      final case class Running(replicators: Map[Topic, TopicReplicator[F]] = Map.empty) extends State

      case object Closed extends State
    }

    for {
      log       <- Log.of[F](Replicator.getClass)
      stateRef  <- SerialRef.of[F, State](State.Running())
      consumer  <- consumerOf(config.consumer) // TODO should not be used directly!
      discovery <- Concurrent[F].start {
        val discovery = for {
          state  <- stateRef.get
          result <- state match {
            case State.Closed     => ().some.pure[F]
            case _: State.Running => for {
              start  <- ClockOf[F].millis
              topics <- consumer.topics
              result <- stateRef.modify[Option[Unit]] {
                case State.Closed         => (State.closed, ().some).pure[F]
                case state: State.Running =>
                  for {
                    end       <- ClockOf[F].millis
                    duration   = end - start
                    topicsNew  = {
                      for {
                        topic <- topics -- state.replicators.keySet
                        if config.topicPrefixes exists topic.startsWith
                      } yield topic
                    }.toList
                    result    <- {
                      if (topicsNew.isEmpty) (state, none[Unit]).pure[F]
                      else {
                        for {
                          _ <- log.info {
                            val topics = topicsNew.mkString(",")
                            s"discover new topics: $topics in ${ duration }ms"
                          }
                          replicators <- Par[F].unorderedSequence {
                            for {
                              topic <- topicsNew
                            } yield for {
                              replicator <- topicReplicatorOf(topic)
                            } yield {
                              (topic, replicator)
                            }
                          }
                        } yield {
                          val state1 = state.copy(replicators = state.replicators ++ replicators)
                          (state1, none[Unit])
                        }
                      }
                    }
                  } yield result
              }
            } yield result
          }
          _ <- result.fold {
            TimerOf[F].sleep(config.topicDiscoveryInterval)
          } {
            _.pure[F]
          }
        } yield result

        val discoveryLoop = discovery.untilDefinedM
        
        Sync[F].guarantee(discoveryLoop)(consumer.close).onError { case error =>
          log.error(s"topics discovery failed with $error", error)
        }
      }
    } yield {

      new Replicator[F] {

        def done = {
          /*for {
            state <- stateRef.get
            _ <- state match {
              case State.Closed         => ().pure[F]
              case state: State.Running => Par[F].unorderedFoldMap(state.replicators.values)(_.done)
            }
            _ <- discovery.join
          } yield {}*/
          // TODO implement properly
          stateRef.get.map {
            case State.Closed     => true
            case _: State.Running => false
          }
        }

        def close = {
          for {
            _ <- stateRef.update {
              case State.Closed         => State.closed.pure[F]
              case state: State.Running => Par[F].unorderedFoldMap(state.replicators.values)(_.close).as(State.closed)
            }
            _ <- discovery.join
          } yield {}
        }
      }
    }
  }


  trait Consumer[F[_]] {

    def topics: F[Set[Topic]]

    def close: F[Unit]
  }

  object Consumer {

    def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

    def apply[F[_] : FlatMap : FromFuture](consumer: skafka.consumer.Consumer[Id, Bytes, Future]): Consumer[F] = {

      new Consumer[F] {

        def topics = {
          for {
            infos <- FromFuture[F].apply { consumer.listTopics() }
          } yield {
            infos.keySet
          }
        }

        def close = {
          FromFuture[F].apply { consumer.close() }
        }
      }
    }
  }


  final case class Metrics[F[_]](
    journal: Option[ReplicatedJournal.Metrics[F]] = None,
    replicator: Option[Topic => TopicReplicator.Metrics[F]] = None, // TODO F ?
    consumer: Option[skafka.consumer.Consumer.Metrics] = None)

  object Metrics {
    def empty[F[_]]: Metrics[F] = Metrics()
  }
}
