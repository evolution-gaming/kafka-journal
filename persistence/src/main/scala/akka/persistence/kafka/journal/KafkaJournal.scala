package akka.persistence.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import cats.Parallel
import cats.effect._
import cats.implicits._
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.{FromFuture, Log, LogOf, ToFuture, ToTry}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.kafka.journal.util.PureConfigHelper._
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.smetrics.MeasureDuration
import com.typesafe.config.Config
import pureconfig.ConfigSource

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.Try

class KafkaJournal(config: Config) extends AsyncWriteJournal { actor =>

  implicit val system       : ActorSystem              = context.system
  implicit val executor     : ExecutionContextExecutor = context.dispatcher
  implicit val contextShift : ContextShift[IO]         = IO.contextShift(executor)
  implicit val parallel     : Parallel[IO]             = IO.ioParallel(contextShift)
  implicit val timer        : Timer[IO]                = IO.timer(executor)
  implicit val toFuture     : ToFuture[IO]             = ToFuture.ioToFuture
  implicit val fromFuture   : FromFuture[IO]           = FromFuture.lift[IO]
  implicit val fromAttempt  : FromAttempt[IO]          = FromAttempt.lift[IO]
  implicit val fromJsResult : FromJsResult[IO]         = FromJsResult.lift[IO]

  lazy val (adapter, release): (JournalAdapter[Future], () => Unit) = {
    val (adapter, release) = await(adapterIO.allocated)
    val adapter1 = adapter.mapK(toFuture.toFunctionK, fromFuture.toFunctionK)
    val release1 = () => await(release)
    (adapter1, release1)
  }

  def await[A](fa: IO[A]): A = {
    val future = toFuture(fa)
    Await.result(future, 1.minute)
  }

  def logOf: Resource[IO, LogOf[IO]] = LogOfFromAkka[IO](system).pure[Resource[IO, *]]

  def randomIdOf: Resource[IO, RandomIdOf[IO]] = RandomIdOf.uuid[IO].pure[Resource[IO, *]]

  def measureDuration: Resource[IO, MeasureDuration[IO]] = MeasureDuration.fromClock(Clock[IO]).pure[Resource[IO, *]]

  def toKey: Resource[IO, ToKey[IO]] = {
    ToKey
      .fromConfig[IO](config)
      .pure[Resource[IO, *]]
  }

  def kafkaJournalConfig: IO[KafkaJournalConfig] = {
    ConfigSource
      .fromConfig(config)
      .load[KafkaJournalConfig]
      .liftTo[IO]
  }

  def origin: IO[Option[Origin]] = {

    val hostName = Origin.hostName[IO]

    def akkaHost = Origin.akkaHost[IO](system)

    def akkaName = Origin.akkaName(system)

    hostName
      .toOptionT
      .orElse(akkaHost.toOptionT)
      .orElse(akkaName.some.toOptionT[IO])
      .value
  }

  def serializer: Resource[IO, EventSerializer[IO, Payload]] = {
    EventSerializer
      .of[IO](system)
      .toResource
  }

  def journalReadWrite(config: KafkaJournalConfig): IO[JournalReadWrite[IO, Payload]] = {
    for {
      jsonCodec <- jsonCodec(config)
    } yield {
      implicit val jsonCodec1 = jsonCodec
      implicit val jsonCodecTry = jsonCodec.mapK(ToTry.functionK)
      JournalReadWrite.of[IO, Payload]
    }
  }

  def metrics: Resource[IO, JournalAdapter.Metrics[IO]] = {
    JournalAdapter.Metrics
      .empty[IO]
      .pure[Resource[IO, *]]
  }

  def appendMetadataOf: Resource[IO, AppendMetadataOf[IO]] = {
    AppendMetadataOf
      .empty[IO]
      .pure[Resource[IO, *]]
  }

  def batching(config: KafkaJournalConfig): Resource[IO, Batching[IO]] = {
    Batching
      .byNumberOfEvents[IO](config.maxEventsInBatch)
      .pure[Resource[IO, *]]
  }

  def cassandraClusterOf: Resource[IO, CassandraClusterOf[IO]] = {
    CassandraClusterOf
      .of[IO]
      .toResource
  }

  def jsonCodec(config: KafkaJournalConfig): IO[JsonCodec[IO]] = {
    val codec: JsonCodec[IO] = config.jsonCodec match {
      case KafkaJournalConfig.JsonCodec.Default  => JsonCodec.default
      case KafkaJournalConfig.JsonCodec.PlayJson => JsonCodec.playJson
      case KafkaJournalConfig.JsonCodec.Jsoniter => JsonCodec.jsoniter
    }
    codec.pure[IO]
  }

  def adapterIO: Resource[IO, JournalAdapter[IO]] = {
    for {
      serializer       <- serializer
      config           <- kafkaJournalConfig.toResource
      journalReadWrite <- journalReadWrite(config).toResource
      adapter          <- adapterIO(config, serializer, journalReadWrite)
    } yield adapter
  }

  def adapterIO[A](
    serializer: EventSerializer[IO, A],
    journalReadWrite: JournalReadWrite[IO, A]
  ): Resource[IO, JournalAdapter[IO]] =
    for {
      config  <- kafkaJournalConfig.toResource
      adapter <- adapterIO(config, serializer, journalReadWrite)
    } yield adapter

  def adapterIO[A](
    config: KafkaJournalConfig,
    serializer: EventSerializer[IO, A],
    journalReadWrite: JournalReadWrite[IO, A]
  ): Resource[IO, JournalAdapter[IO]] = {
    val resource = for {
      logOf              <- logOf
      log                <- logOf(classOf[KafkaJournal]).toResource
      _                  <- log.debug(s"config: $config").toResource
      randomId           <- randomIdOf
      measureDuration    <- measureDuration
      toKey              <- toKey
      origin             <- origin.toResource
      appendMetadataOf   <- appendMetadataOf
      metrics            <- metrics
      batching           <- batching(config)
      cassandraClusterOf <- cassandraClusterOf
      jsonCodec          <- jsonCodec(config).toResource
      adapter            <- adapterOf(
        toKey              = toKey,
        origin             = origin,
        serializer         = serializer,
        journalReadWrite   = journalReadWrite,
        config             = config,
        metrics            = metrics,
        appendMetadataOf   = appendMetadataOf,
        batching           = batching,
        log                = log,
        cassandraClusterOf = cassandraClusterOf)(
        logOf              = logOf,
        randomIdOf         = randomId,
        measureDuration    = measureDuration,
        jsonCodec          = jsonCodec)
    } yield {
      (adapter, log)
    }

    val result = for {
      a <- resource.allocated.timeout(config.startTimeout)
    } yield {
      val ((adapter, log), release) = a
      val release1 = release.timeout(config.startTimeout).handleErrorWith { error =>
        log.error(s"release failed with $error", error)
      }
      (adapter, release1)
    }

    Resource(result)
  }

  def adapterOf[A](
    toKey: ToKey[IO],
    origin: Option[Origin],
    serializer: EventSerializer[IO, A],
    journalReadWrite: JournalReadWrite[IO, A],
    config: KafkaJournalConfig,
    metrics: JournalAdapter.Metrics[IO],
    appendMetadataOf: AppendMetadataOf[IO],
    batching: Batching[IO],
    log: Log[IO],
    cassandraClusterOf: CassandraClusterOf[IO])(implicit
    logOf: LogOf[IO],
    randomIdOf: RandomIdOf[IO],
    measureDuration: MeasureDuration[IO],
    jsonCodec: JsonCodec[IO]
  ): Resource[IO, JournalAdapter[IO]] = {

    JournalAdapter.of[IO, A](
      toKey = toKey,
      origin = origin,
      serializer = serializer,
      journalReadWrite = journalReadWrite,
      config = config,
      metrics = metrics,
      log = log,
      batching = batching,
      appendMetadataOf = appendMetadataOf,
      cassandraClusterOf = cassandraClusterOf)
  }

  override def preStart(): Unit = {
    super.preStart()
    val _ = adapter
  }

  override def postStop(): Unit = {
    release()
    super.postStop()
  }

  def asyncWriteMessages(atomicWrites: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    Future { adapter.write(atomicWrites) }.flatten
  }

  def asyncDeleteMessagesTo(persistenceId: PersistenceId, to: Long): Future[Unit] = {
    SeqNr.opt(to) match {
      case Some(to) => adapter.delete(persistenceId, to.toDeleteTo)
      case None     => Future.unit
    }
  }

  def asyncReplayMessages(
    persistenceId: PersistenceId,
    from: Long,
    to: Long,
    max: Long)(
    f: PersistentRepr => Unit
  ): Future[Unit] = {

    val seqNrFrom = SeqNr.of[Option](from) getOrElse SeqNr.min
    val seqNrTo = SeqNr.of[Option](to) getOrElse SeqNr.max
    val range = SeqRange(seqNrFrom, seqNrTo)
    val f1 = (a: PersistentRepr) => Future.fromTry(Try { f(a) })
    adapter.replay(persistenceId, range, max)(f1)
  }

  def asyncReadHighestSequenceNr(persistenceId: PersistenceId, from: Long): Future[Long] = {
    val seqNr = SeqNr.of[Option](from) getOrElse SeqNr.min
    for {
      seqNr <- adapter.lastSeqNr(persistenceId, seqNr)
    } yield seqNr match {
      case Some(seqNr) => seqNr.value
      case None        => from
    }
  }
}