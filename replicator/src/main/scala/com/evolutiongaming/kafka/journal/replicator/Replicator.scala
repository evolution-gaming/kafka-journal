package com.evolutiongaming.kafka.journal.replicator


import akka.actor.ActorSystem
import cats.effect._
import cats.implicits._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.ReplicatedJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraCluster, CassandraSession, ReplicatedCassandra}
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.kafka.journal.util.ClockHelper._
import com.evolutiongaming.kafka.journal.util._
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{Topic, Bytes => _}

import scala.concurrent.ExecutionContext

// TODO TEST
trait Replicator[F[_]] {

  def done: F[Boolean]
}
 
object Replicator {

  def of[F[_] : Concurrent : Timer : Par : FromFuture : ToFuture : ContextShift](
    system: ActorSystem,
    metrics: Metrics[F] = Metrics.empty[F]): Resource[F, Replicator[F]] = {

    val config = Sync[F].delay {
      val config = system.settings.config.getConfig("evolutiongaming.kafka-journal.replicator")
      ReplicatorConfig(config)
    }

    for {
      config     <- Resource.liftF(config)
      cassandra  <- CassandraCluster.of(config.cassandra.client, config.cassandra.retries)
      session    <- cassandra.session
      blocking    = Sync[F].delay { system.dispatchers.lookup(config.blockingDispatcher) /*TODO move to common place*/}
      blocking   <- Resource.liftF(blocking)
      replicator <- {
        implicit val session1 = session
        of[F](config, blocking, metrics)
      }
    } yield replicator
  }

  def of[F[_] : Concurrent : Timer : Par : FromFuture : ToFuture : ContextShift : CassandraSession](
    config: ReplicatorConfig,
    blocking: ExecutionContext,
    metrics: Metrics[F]): Resource[F, Replicator[F]] = {

    implicit val clock = Timer[F].clock

    for {
      journal <- Resource.liftF(ReplicatedCassandra.of[F](config.cassandra, metrics.journal))
      result  <- {
        implicit val journal1 = journal
        of2(config, blocking, metrics)
      }
    } yield result
  }

  def of2[F[_] : Concurrent : Timer : Par : FromFuture : ReplicatedJournal : ContextShift](
    config: ReplicatorConfig,
    blocking: ExecutionContext,
    metrics: Metrics[F]): Resource[F, Replicator[F]] = {

    val kafkaConsumerOf = (config: ConsumerConfig) => {
      KafkaConsumer.of[F, Id, Bytes](config, blocking, metrics.consumer)
    }

    val consumerOf = kafkaConsumerOf.andThen { consumer =>
      for {
        consumer <- consumer.allocated
      } yield {
        val (resource, release) = consumer
        Consumer[F](resource, release)
      }
    }

    val topicReplicatorOf = (topic: Topic) => {
      val prefix = config.consumer.groupId getOrElse "journal-replicator"
      val groupId = s"$prefix-$topic"
      val consumerConfig = config.consumer.copy(
        groupId = Some(groupId),
        autoOffsetReset = AutoOffsetReset.Earliest,
        autoCommit = false)

      val consumer = for {
        consumer <- kafkaConsumerOf(consumerConfig)
      } yield {
        TopicReplicator.Consumer[F](consumer, config.pollTimeout)
      }

      implicit val metrics1 = metrics.replicator.fold(TopicReplicator.Metrics.empty[F]) { _.apply(topic) }

      TopicReplicator.of[F](topic = topic, consumer = consumer)
    }

    of(config, consumerOf, topicReplicatorOf)
  }

  def of[F[_] : Concurrent : Timer : Par : FromFuture](
    config: ReplicatorConfig,
    consumerOf: ConsumerConfig => F[Consumer[F]],
    topicReplicatorOf: Topic => F[TopicReplicator[F]]): Resource[F, Replicator[F]] = {

    implicit val clock: Clock[F] = Timer[F].clock

    sealed trait State

    object State {

      def closed: State = Closed

      final case class Running(replicators: Map[Topic, TopicReplicator[F]] = Map.empty) extends State

      case object Closed extends State
    }

    val result = for {
      log       <- Log.of[F](Replicator.getClass)
      stateRef  <- SerialRef.of[F, State](State.Running())
      consumer  <- consumerOf(config.consumer) // TODO should not be used directly!
      discovery <- Concurrent[F].start {
        val discovery = for {
          state  <- stateRef.get
          result <- state match {
            case State.Closed     => ().some.pure[F]
            case _: State.Running => for {
              start  <- Clock[F].millis
              topics <- consumer.topics
              result <- stateRef.modify[Option[Unit]] {
                case State.Closed         => (State.closed, ().some).pure[F]
                case state: State.Running =>
                  for {
                    end       <- Clock[F].millis
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
                          replicators <- Par[F].sequence {
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
            Timer[F].sleep(config.topicDiscoveryInterval)
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

      val release = {
        for {
          _ <- stateRef.update {
            case State.Closed         => State.closed.pure[F]
            case state: State.Running => Par[F].foldMap(state.replicators.values)(_.close).as(State.closed)
          }
          _ <- discovery.join
        } yield {}
      }

      val result = new Replicator[F] {

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
      }

      (result, release)
    }

    Resource(result)
  }


  trait Consumer[F[_]] {

    def topics: F[Set[Topic]]

    def close: F[Unit] // TODO resource
  }

  object Consumer {

    def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

    def apply[F[_]](consumer: KafkaConsumer[F, Id, Bytes], release: F[Unit]): Consumer[F] = {
      new Consumer[F] {

        def topics = consumer.topics

        def close = release
      }
    }
  }


  final case class Metrics[F[_]](
    journal: Option[ReplicatedJournal.Metrics[F]] = None,
    replicator: Option[Topic => TopicReplicator.Metrics[F]] = None,
    consumer: Option[skafka.consumer.Consumer.Metrics] = None)

  object Metrics {
    def empty[F[_]]: Metrics[F] = Metrics()
  }
}
