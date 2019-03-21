package akka.persistence.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import cats.effect._
import cats.implicits._
import cats.{Parallel, ~>}
import com.evolutiongaming.catshelper.{Log, LogOf, ToFuture}
import com.evolutiongaming.kafka.journal._
import com.typesafe.config.Config

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try

class KafkaJournal(config: Config) extends AsyncWriteJournal {

  implicit val system: ActorSystem                = context.system
  implicit val executor: ExecutionContextExecutor = context.dispatcher

  implicit val contextShift: ContextShift[IO]     = IO.contextShift(executor)
  implicit val parallel    : Parallel[IO, IO.Par] = IO.ioParallel(contextShift)
  implicit val timer       : Timer[IO]            = IO.timer(executor)
  implicit val logOf       : LogOf[IO]            = LogOfFromAkka[IO](system)
  implicit val randomId    : RandomId[IO]         = RandomId.uuid[IO]

  lazy val (adapter, release): (JournalAdapter[Future], () => Unit) = {
    
    val (adapter, release) = adapterIO.unsafeRunSync()

    val toFuture = new (IO ~> Future) {
      def apply[A](fa: IO[A]) = ToFuture[IO].apply(fa)
    }
    val adapter1 = adapter.mapK(toFuture)
    val release1 = () => release.unsafeRunSync()
    (adapter1, release1)
  }

  override def preStart(): Unit = {
    super.preStart()
    val _ = adapter
  }

  override def postStop(): Unit = {
    release()
    super.postStop()
  }

  def toKey: IO[ToKey] = ToKey(config).pure[IO]

  def kafkaJournalConfig: IO[KafkaJournalConfig] = KafkaJournalConfig(config).pure[IO]

  def origin: IO[Option[Origin]] = {
    Sync[IO].delay {
      val origin = Origin.HostName orElse Origin.AkkaHost(system) getOrElse Origin.AkkaName(system)
      origin.some
    }
  }

  def serializer: IO[EventSerializer[cats.Id]] = EventSerializer.unsafe(system).pure[IO]

  def metrics: IO[JournalAdapter.Metrics[IO]] = JournalAdapter.Metrics.empty[IO].pure[IO]

  def metadataAndHeadersOf: IO[MetadataAndHeadersOf[IO]] = MetadataAndHeadersOf.empty[IO].pure[IO]

  def batching(config: KafkaJournalConfig): IO[Batching[IO]] = {
    Batching.byNumberOfEvents[IO](config.maxEventsInBatch).pure[IO]
  }

  def adapterIO: IO[(JournalAdapter[IO], IO[Unit])] = {
    for {
      config  <- kafkaJournalConfig
      adapter <- adapterIO(config)
    } yield adapter
  }

  def adapterIO(config: KafkaJournalConfig): IO[(JournalAdapter[IO], IO[Unit])] = {
    val result = for {
      log                  <- logOf(classOf[KafkaJournal])
      _                    <- log.debug(s"config: $config")
      toKey                <- toKey
      origin               <- origin
      metadataAndHeadersOf <- metadataAndHeadersOf
      serializer           <- serializer
      metrics              <- metrics
      batching             <- batching(config)
      adapter               = adapterOf(
        toKey                = toKey,
        origin               = origin,
        serializer           = serializer,
        config               = config,
        metrics              = metrics,
        metadataAndHeadersOf = metadataAndHeadersOf,
        batching             = batching,
        log                  = log)
      ab                   <- adapter.allocated
    } yield {
      val (adapter, release) = ab
      val release1 = release.timeout(config.startTimeout).handleErrorWith { error =>
        log.error(s"release failed with $error", error)
      }
      (adapter, release1)
    }
    result.timeout(config.startTimeout)
  }

  def adapterOf(
    toKey: ToKey,
    origin: Option[Origin],
    serializer: EventSerializer[cats.Id],
    config: KafkaJournalConfig,
    metrics: JournalAdapter.Metrics[IO],
    metadataAndHeadersOf: MetadataAndHeadersOf[IO],
    batching: Batching[IO],
    log: Log[IO]
  ): Resource[IO, JournalAdapter[IO]] = {

    JournalAdapter.of[IO](
      toKey = toKey,
      origin = origin,
      serializer = serializer,
      config = config,
      metrics = metrics,
      log = log,
      batching = batching,
      metadataAndHeadersOf = metadataAndHeadersOf,
      executor = executor)
  }

  def asyncWriteMessages(atomicWrites: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    Future { adapter.write(atomicWrites) }.flatten
  }

  def asyncDeleteMessagesTo(persistenceId: PersistenceId, to: Long): Future[Unit] = {
    SeqNr.opt(to) match {
      case Some(to) => adapter.delete(persistenceId, to)
      case None     => Future.unit
    }
  }

  def asyncReplayMessages(persistenceId: PersistenceId, from: Long, to: Long, max: Long)
    (f: PersistentRepr => Unit): Future[Unit] = {

    val seqNrFrom = SeqNr(from, SeqNr.Min)
    val seqNrTo = SeqNr(to, SeqNr.Max)
    val range = SeqRange(seqNrFrom, seqNrTo)
    adapter.replay(persistenceId, range, max)(f)
  }

  def asyncReadHighestSequenceNr(persistenceId: PersistenceId, from: Long): Future[Long] = {
    val seqNr = SeqNr(from, SeqNr.Min)
    for {
      seqNr <- adapter.lastSeqNr(persistenceId, seqNr)
    } yield seqNr match {
      case Some(seqNr) => seqNr.value
      case None        => from
    }
  }
}