package akka.persistence.kafka.journal

import akka.persistence.PluginSpec
import com.evolutiongaming.cassandra.StartCassandra
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.StartKafka
import com.evolutiongaming.kafka.journal.replicator.Replicator

import scala.concurrent.duration._
import scala.util.control.NonFatal

trait KafkaPluginSpec extends PluginSpec {

  var shutdown: () => Async[Unit] = () => Async.unit

  override def beforeAll(): Unit = {
    KafkaPluginSpec.start()
    val replicator = Replicator(system, system.dispatcher)
    shutdown = () => replicator.shutdown()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()

    KafkaPluginSpec.safe {
      shutdown().await(1.minute)
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

