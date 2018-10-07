package com.evolutiongaming.kafka.journal.replicator


import akka.actor.ActorSystem
import com.evolutiongaming.cassandra.{CreateCluster, Session}
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.concurrent.serially.SeriallyAsync
import com.evolutiongaming.kafka.journal.AsyncHelper._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.ReplicatedJournal
import com.evolutiongaming.kafka.journal.eventual.cassandra.ReplicatedCassandra
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{Topic, Bytes => _}

import scala.compat.Platform
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

trait Replicator {

  def running(): Boolean

  def shutdown(): Async[Unit]
}

object Replicator {

  def apply(
    system: ActorSystem,
    metrics: Metrics[Async] = Metrics.empty[Async]): Async[Replicator] = safe {

    val name = "evolutiongaming.kafka-journal.replicator"
    val config = ReplicatorConfig(system.settings.config.getConfig(name))
    val cassandra = CreateCluster(config.cassandra.client)
    val ecBlocking = system.dispatchers.lookup(s"$name.blocking-dispatcher")
    implicit val ec = system.dispatcher
    for {
      session <- cassandra.connect().async
    } yield {
      val replicator = apply(config, session, ecBlocking, metrics)(system, ec)

      new Replicator {

        def running() = {
          replicator.running()
        }

        def shutdown() = {
          for {
            _ <- replicator.shutdown()
            _ <- cassandra.close().async
          } yield {}
        }
      }
    }
  }

  def apply(
    config: ReplicatorConfig,
    session: Session,
    ecBlocking: ExecutionContext,
    metrics: Metrics[Async])(implicit
    system: ActorSystem,
    ec: ExecutionContext): Replicator = {

    val consumerOf = (config: ConsumerConfig) => {
      val consumer = Consumer[Id, Bytes](config, ecBlocking)
      metrics.consumer.fold(consumer) { Consumer(consumer, _) }
    }

    val journal = {
      val journal = ReplicatedCassandra(session, config.cassandra)
      val actorLog = ActorLog(system, ReplicatedCassandra.getClass)
      val logging = ReplicatedJournal(journal, Log(actorLog))
      metrics.journal.fold(logging) { ReplicatedJournal(logging, _) }
    }

    val createReplicator = (topic: Topic) => {
      val prefix = config.consumer.groupId getOrElse "journal-replicator"
      val groupId = s"$prefix-$topic"
      val consumerConfig = config.consumer.copy(groupId = Some(groupId))
      val consumer = consumerOf(consumerConfig)
      val kafkaConsumer = KafkaConsumer(consumer, config.pollTimeout)
      TopicReplicator(
        topic = topic,
        journal = journal,
        consumer = kafkaConsumer,
        metrics = metrics.replicator.fold(TopicReplicator.Metrics.empty(Async.unit)) { _.apply(topic) })
    }

    apply(config, session, consumerOf, createReplicator)
  }

  def apply(
    config: ReplicatorConfig,
    session: Session,
    consumerOf: ConsumerConfig => Consumer[Id, Bytes],
    topicReplicatorOf: Topic => TopicReplicator[Async])(implicit
    system: ActorSystem, ec: ExecutionContext): Replicator = {

    val log = ActorLog(system, Replicator.getClass)
    val serially = SeriallyAsync()
    val stateVar = AsyncVar[State](State.Running.Empty, serially)
    val consumer = consumerOf(config.consumer)

    def discoverTopics(): Unit = {
      val timestamp = Platform.currentTime
      val result = stateVar.updateAsync {
        case State.Stopped        => State.Stopped.async
        case state: State.Running =>
          for {
            topics <- consumer.listTopics().async
          } yield {
            val duration = Platform.currentTime - timestamp
            val topicsNew = for {
              (topic, _) <- topics -- state.replicators.keySet
              if config.topicPrefixes.exists(topic.startsWith)
            } yield {
              topic
            }

            val result = {
              if (topicsNew.isEmpty) state
              else {
                def topicsStr = topicsNew.toSeq.sorted.mkString(",")

                log.info(s"discover new topics: $topicsStr in ${ duration }ms")

                val replicatorsNew = for {
                  topic <- topicsNew
                } yield {
                  val replicator = topicReplicatorOf(topic)
                  (topic, replicator)
                }

                state.copy(replicators = state.replicators ++ replicatorsNew)
              }
            }
            system.scheduler.scheduleOnce(config.topicDiscoveryInterval) {
              if (stateVar.value() != State.Stopped) discoverTopics()
            }
            result
          }
      }
      result.onFailure { failure => log.error(s"discoverTopics failed $failure", failure) }
    }

    discoverTopics()

    new Replicator {

      def running() = {
        stateVar.value() match {
          case State.Stopped        => false
          case state: State.Running => state.replicators.values.forall(_.done().value().isEmpty)
        }
      }

      def shutdown() = {

        def shutdownReplicators() = {
          stateVar.updateAsync {
            case State.Stopped        => State.Stopped.async
            case state: State.Running =>
              val shutdowns = state.replicators.values.toList.map(_.shutdown())
              for {_ <- Async.foldUnit(shutdowns)} yield State.Stopped
          }
        }

        for {
          _ <- shutdownReplicators()
          //          _ <- serially.stop().async
          kafka = consumer.close().async
          _ <- session.close().async
          _ <- kafka
        } yield {}
      }
    }
  }

  private def safe[T](f: => Async[T]): Async[T] = {
    try f catch { case NonFatal(failure) => Async.failed(failure) }
  }


  sealed trait State

  object State {

    final case class Running(replicators: Map[Topic, TopicReplicator[Async]] = Map.empty) extends State

    object Running {
      val Empty: Running = Running()
    }


    case object Stopped extends State
  }


  final case class Metrics[F[_]](
    journal: Option[ReplicatedJournal.Metrics[F]] = None,
    replicator: Option[Topic => TopicReplicator.Metrics[F]] = None,
    consumer: Option[Consumer.Metrics] = None)

  object Metrics {
    def empty[F[_]]: Metrics[F] = Metrics()
  }
}
