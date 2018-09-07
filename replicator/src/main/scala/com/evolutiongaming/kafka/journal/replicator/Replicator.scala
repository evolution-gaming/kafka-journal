package com.evolutiongaming.kafka.journal.replicator


import akka.actor.ActorSystem
import com.evolutiongaming.cassandra.CreateCluster
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.concurrent.serially.SeriallyAsync
import com.evolutiongaming.kafka.journal.AsyncHelper._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.cassandra.ReplicatedCassandra
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{Partition, Topic, Bytes => _}

import scala.compat.Platform
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

trait Replicator {
  def shutdown(): Async[Unit]
}

// TODO refactor EventualDb api, make sure it does operate with Seq[Actions]
object Replicator {

  def apply(system: ActorSystem): Replicator = {
    val name = "evolutiongaming.kafka-journal.replicator"
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

    def createReplicator(topic: Topic, partitions: Set[Partition]) = {
      val prefix = config.consumer.groupId getOrElse "journal-replicator"
      val groupId = s"$prefix-$topic"
      val consumerConfig = config.consumer.copy(groupId = Some(groupId))
      val consumer = CreateConsumer[String, Bytes](consumerConfig, ecBlocking)
      implicit val kafkaConsumer = KafkaConsumer(consumer, 100.millis /*TODO from configuration*/)
      val actorLog = ActorLog(system, TopicReplicator.getClass) prefixed topic
      implicit val log = Log(actorLog)
      val stopRef = Ref[Boolean, Async]()
      //      val currentTime = IO[Async].point(Platform.currentTime) // TODO
      TopicReplicator(topic, partitions, kafkaConsumer, journal, log, stopRef)
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
            val topicsNew = for {
              (topic, infos) <- topics -- state.replicators.keySet
              if config.topicPrefixes.exists(topic.startsWith)
            } yield {
              (topic, infos)
            }

            val result = {
              if (topicsNew.isEmpty) state
              else {
                def topicsStr = topicsNew.keys.mkString(",")

                log.info(s"discover new topics: $topicsStr in ${ duration }ms")

                val replicatorsNew = for {
                  (topic, infos) <- topicsNew
                } yield {
                  val partitions = for {info <- infos} yield info.partition
                  val replicator = createReplicator(topic, partitions.toSet)
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

    final case class Running(replicators: Map[Topic, TopicReplicator[Async]] = Map.empty) extends State

    object Running {
      val Empty: Running = Running()
    }


    case object Stopped extends State
  }
}
