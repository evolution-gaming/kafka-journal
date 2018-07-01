package akka.persistence.kafka.journal

import akka.persistence.PluginSpec
import akka.persistence.kafka.journal.replicator.Replicator
import com.evolutiongaming.cassandra.StartCassandra
import com.evolutiongaming.kafka.StartKafka

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NonFatal

trait KafkaPluginSpec extends PluginSpec {

  var shutdownKafka: StartKafka.Shutdown = StartKafka.Shutdown.Empty
  var shutdownCassandra: StartKafka.Shutdown = StartCassandra.Shutdown.Empty
  var shutdownReplicator: Replicator.Shutdown = Replicator.Shutdown.Empty

  override def beforeAll(): Unit = {
    shutdownKafka = StartKafka()
    shutdownCassandra = StartCassandra()
    shutdownReplicator = Replicator(system, system.dispatcher)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()

    def safe[T](f: => T) = {
      try f catch {
        case NonFatal(failure) => failure.printStackTrace() // TODO use logger
      }
    }

    safe {
      Await.result(shutdownReplicator(), 1.minute) // TODO
    }

    safe {
      shutdownKafka()
    }
    safe {
      shutdownCassandra()
    }
  }
}

