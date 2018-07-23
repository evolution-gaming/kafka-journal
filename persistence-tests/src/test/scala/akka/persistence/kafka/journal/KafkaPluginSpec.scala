package akka.persistence.kafka.journal

import akka.persistence.PluginSpec
import com.evolutiongaming.cassandra.StartCassandra
import com.evolutiongaming.kafka.StartKafka
import com.evolutiongaming.kafka.journal.replicator.Replicator

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.NonFatal

trait KafkaPluginSpec extends PluginSpec {

  var shutdownReplicator: Replicator.Shutdown = Replicator.Shutdown.Empty

  override def beforeAll(): Unit = {
    KafkaPluginSpec.start()
    shutdownReplicator = Replicator(system, system.dispatcher)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()

    KafkaPluginSpec.safe {
      Await.result(shutdownReplicator(), 1.minute)
    }
  }
}

object KafkaPluginSpec {

  private lazy val started = {
    val shutdownCassandra = StartCassandra()
    val shutdownKafka = StartKafka()

    sys.addShutdownHook {
      safe {
        shutdownCassandra()
      }

      safe {
        shutdownKafka()
      }
    }
  }

  def start(): Unit = started

  def safe[T](f: => T): Unit = {
    try f catch {
      case NonFatal(failure) => failure.printStackTrace()
    }
  }
}

