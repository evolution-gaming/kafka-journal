package com.evolutiongaming.kafka.journal.replicator

import java.util.UUID

import akka.actor.ActorSystem
import com.evolutiongaming.cassandra.CreateCluster
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.concurrent.serially.SeriallyAsync
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.cassandra.ReplicatedCassandra
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{Topic, Bytes => _}

import scala.compat.Platform
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

trait Replicator {
  def shutdown(): Async[Unit]
}

// TODO refactor EventualDb api, make sure it does operate with Seq[Actions]
object Replicator {

  def apply(system: ActorSystem): Replicator = {
    val name = "kafka-journal.replicator"
    val config = ReplicatorConfig(system.settings.config.getConfig(name))
    val ecBlocking = system.dispatchers.lookup(s"$name.blocking-dispatcher")
    apply(config, ecBlocking)(system, system.dispatcher)
  }

  def apply(config: ReplicatorConfig, ecBlocking: ExecutionContext)(implicit system: ActorSystem, ec: ExecutionContext): Replicator = {
    val log = ActorLog(system, Replicator.getClass)
    val cluster = CreateCluster(config.cassandra.client)
    val session = Await.result(cluster.connect(), 5.seconds) // TODO handle this properly
    val journal = ReplicatedCassandra(session, config.cassandra)

    val serially = SeriallyAsync()
    val stateVar = AsyncVar[State](State.Running.Empty, serially)

    def createReplicator(topic: Topic) = {
      val uuid = UUID.randomUUID()
      val prefix = config.consumer.groupId getOrElse "journal-replicator"
      val groupId = s"$prefix-$topic-$uuid"
      val consumerConfig = config.consumer.copy(groupId = Some(groupId))
      val consumer = CreateConsumer[String, Bytes](consumerConfig, ecBlocking)

      val log = ActorLog(system, TopicReplicator.getClass) prefixed topic
      TopicReplicator(topic, consumer, journal, log)
    }

    val consumer = CreateConsumer[String, Bytes](config.consumer, ecBlocking)

    def discoverTopics(): Unit = {
      val timestamp = Platform.currentTime
      val result = stateVar.updateAsync {
        case State.Stopped        => State.Stopped.async
        case state: State.Running =>
          for {
            topics <- consumer.listTopics().async
          } yield {
            val duration = Platform.currentTime - timestamp
            val topicsNew = (topics.keySet -- state.replicators.keySet).filter { topic =>
              config.topicPrefixes.exists(topic.startsWith)
            }

            val result = {
              if (topicsNew.isEmpty) state
              else {
                log.info(s"discover new topics: ${ topicsNew.mkString(",") } in ${ duration }ms")

                val replicators = topics.keys.foldLeft(state.replicators) { (replicators, topic) =>
                  if (replicators contains topic) {
                    replicators
                  } else {
                    val replicator = createReplicator(topic)
                    replicators.updated(topic, replicator)
                  }
                }
                state.copy(replicators = replicators)
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
          _ <- serially.stop().async
          kafka = consumer.close().async
          _ <- cluster.close().async
          _ <- kafka
        } yield {}
      }
    }
  }


  sealed trait State

  object State {

    final case class Running(replicators: Map[Topic, TopicReplicator] = Map.empty) extends State

    object Running {
      val Empty: Running = Running()
    }


    case object Stopped extends State
  }
}
