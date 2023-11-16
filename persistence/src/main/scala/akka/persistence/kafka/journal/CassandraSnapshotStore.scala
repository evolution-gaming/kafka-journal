package akka.persistence.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.syntax.all._
import cats.effect.unsafe.{IORuntime, IORuntimeConfig}
import cats.syntax.all._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{FromFuture, LogOf, ToFuture}
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.kafka.journal.util.PureConfigHelper._
import com.evolutiongaming.kafka.journal.{JsonCodec, LogOfFromAkka, Origin, Payload, SnapshotReadWrite}
import com.evolutiongaming.retry.Retry.implicits._
import com.evolutiongaming.retry.{OnError, Strategy}
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.typesafe.config.Config
import pureconfig.ConfigSource

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

class CassandraSnapshotStore(config: Config) extends SnapshotStore { actor =>

  implicit val system: ActorSystem = context.system
  implicit val executor: ExecutionContextExecutor = context.dispatcher

  private val (blocking, blockingShutdown) = IORuntime.createDefaultBlockingExecutionContext("kafka-journal-blocking")
  private val (scheduler, schedulerShutdown) = IORuntime.createDefaultScheduler("kafka-journal-scheduler")
  implicit val ioRuntime: IORuntime = IORuntime(
    compute = executor,
    blocking = blocking,
    scheduler = scheduler,
    shutdown = () => {
      blockingShutdown()
      schedulerShutdown()
    },
    config = IORuntimeConfig()
  )
  implicit val toFuture: ToFuture[IO] = ToFuture.ioToFuture
  implicit val fromFuture: FromFuture[IO] = FromFuture.lift[IO]

  val adapter: Future[(SnapshotStoreAdapter[Future], IO[Unit])] =
    adapterIO
      .map { _.mapK(toFuture.toFunctionK, fromFuture.toFunctionK) }
      .allocated
      .toFuture

  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] =
    ???

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = ???

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = ???

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = ???

  def adapterIO: Resource[IO, SnapshotStoreAdapter[IO]] = {
    for {
      snapshotSerializer <- serializer
      config <- kafkaJournalConfig.toResource
      snapshotReadWrite <- snapshotReadWrite(config).toResource
      adapter <- adapterIO(config, snapshotSerializer, snapshotReadWrite)
    } yield adapter
  }

  def adapterIO[A](
    snapshotSerializer: SnapshotSerializer[IO, A],
    snapshotReadWrite: SnapshotReadWrite[IO, A]
  ): Resource[IO, SnapshotStoreAdapter[IO]] = {
    for {
      config <- kafkaJournalConfig.toResource
      adapter <- adapterIO(config, snapshotSerializer, snapshotReadWrite)
    } yield adapter
  }

  def adapterIO[A](
    config: KafkaJournalConfig,
    snapshotSerializer: SnapshotSerializer[IO, A],
    snapshotReadWrite: SnapshotReadWrite[IO, A]
  ): Resource[IO, SnapshotStoreAdapter[IO]] = {
    for {
      logOf <- logOf
      log <- logOf(classOf[KafkaJournal]).toResource
      _ <- log.debug(s"config: $config").toResource
      adapter <- Resource {
        val adapter = for {
          toKey <- toKey
          origin <- origin.toResource
          cassandraClusterOf <- cassandraClusterOf
          adapter <- adapterOf(
            toKey = toKey,
            origin = origin,
            snapshotSerializer = snapshotSerializer,
            snapshotReadWrite = snapshotReadWrite,
            config = config,
            cassandraClusterOf = cassandraClusterOf
          )(logOf = logOf)
        } yield adapter
        val strategy = Strategy.fibonacci(100.millis).cap(config.startTimeout)
        val onError: OnError[IO, Throwable] = { (error, status, decision) =>
          {
            decision match {
              case OnError.Decision.Retry(delay) =>
                log.warn(s"allocate failed, retrying in $delay, error: $error")

              case OnError.Decision.GiveUp =>
                val retries = status.retries
                val duration = status.delay
                log.error(s"allocate failed after $retries retries within $duration: $error", error)
            }
          }
        }
        adapter.allocated
          .retry(strategy, onError)
          .timeout(config.startTimeout)
          .map { case (adapter, release0) =>
            val release = release0
              .timeout(config.startTimeout)
              .handleErrorWith { error => log.error(s"release failed with $error", error) }
            (adapter, release)
          }
      }
    } yield adapter
  }

  def adapterOf[A](
    toKey: ToKey[IO],
    origin: Option[Origin],
    snapshotSerializer: SnapshotSerializer[IO, A],
    snapshotReadWrite: SnapshotReadWrite[IO, A],
    config: KafkaJournalConfig,
    cassandraClusterOf: CassandraClusterOf[IO]
  )(implicit logOf: LogOf[IO]): Resource[IO, SnapshotStoreAdapter[IO]] =
    SnapshotStoreAdapter.of[IO, A](
      toKey = toKey,
      origin = origin,
      snapshotSerializer = snapshotSerializer,
      snapshotReadWrite = snapshotReadWrite,
      config = config,
      cassandraClusterOf = cassandraClusterOf
    )

  def toKey: Resource[IO, ToKey[IO]] =
    ToKey.fromConfig[IO](config).pure[Resource[IO, *]]

  def origin: IO[Option[Origin]] = {
    val hostName = Origin.hostName[IO]
    def akkaHost = Origin.akkaHost[IO](system)
    def akkaName = Origin.akkaName(system)
    hostName.toOptionT
      .orElse(akkaHost.toOptionT)
      .orElse(akkaName.some.toOptionT[IO])
      .value
  }

  def kafkaJournalConfig: IO[KafkaJournalConfig] =
    ConfigSource
      .fromConfig(config)
      .load[KafkaJournalConfig]
      .liftTo[IO]

  def serializer: Resource[IO, SnapshotSerializer[IO, Payload]] = {
    ??? // SnapshotSerializer.of[IO](system).toResource
  }

  def snapshotReadWrite(config: KafkaJournalConfig): IO[SnapshotReadWrite[IO, Payload]] =
    for {
      jsonCodec <- jsonCodec(config)
    } yield {
      implicit val jsonCodec1 = jsonCodec
      SnapshotReadWrite.of[IO, Payload]
    }

  def jsonCodec(config: KafkaJournalConfig): IO[JsonCodec[IO]] = {
    val codec: JsonCodec[IO] = config.jsonCodec match {
      case KafkaJournalConfig.JsonCodec.Default  => JsonCodec.default
      case KafkaJournalConfig.JsonCodec.PlayJson => JsonCodec.playJson
      case KafkaJournalConfig.JsonCodec.Jsoniter => JsonCodec.jsoniter
    }
    codec.pure[IO]
  }

  def cassandraClusterOf: Resource[IO, CassandraClusterOf[IO]] =
    CassandraClusterOf.of[IO].toResource

  def logOf: Resource[IO, LogOf[IO]] =
    LogOfFromAkka[IO](system).pure[Resource[IO, *]]

}
