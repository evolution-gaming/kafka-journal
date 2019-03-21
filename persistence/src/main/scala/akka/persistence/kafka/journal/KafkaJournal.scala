package akka.persistence.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import cats.effect._
import cats.implicits._
import cats.{Parallel, ~>}
import com.evolutiongaming.catshelper.ToFuture
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.safeakka.actor.ActorLog
import com.typesafe.config.Config

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try
import scala.util.control.NonFatal

class KafkaJournal(config: Config) extends AsyncWriteJournal {

  implicit val system: ActorSystem = context.system
  implicit val executor: ExecutionContextExecutor = context.dispatcher

  implicit val contextShift: ContextShift[IO] = IO.contextShift(executor)
  implicit val parallel: Parallel[IO, IO.Par] = IO.ioParallel(contextShift)
  implicit val timer: Timer[IO]               = IO.timer(executor)
  implicit val logOf: LogOf[IO]               = LogOf[IO](system)
  implicit val randomId: RandomId[IO]         = RandomId.uuid[IO]

  val log: ActorLog = ActorLog(system, classOf[KafkaJournal])

  lazy val (adapter, release): (JournalAdapter[Future], () => Unit) = {
    val config = kafkaJournalConfig()

    log.debug(s"Config: $config")

    val resource = {
      val adapter = for {
        toKey                <- toKey
        origin               <- origin
        metadataAndHeadersOf <- metadataAndHeadersOf
        serializer           <- serializer
        metrics              <- metrics
        batching             <- batching(config)
      } yield {
        adapterOf(
          toKey                = toKey,
          origin               = origin,
          serializer           = serializer,
          config               = config,
          metrics              = metrics,
          metadataAndHeadersOf = metadataAndHeadersOf,
          batching             = batching)
      }
      Resource.liftF(adapter).flatten
    }

    val timeout = config.startTimeout
    val (adapter, release) = resource.allocated.unsafeRunTimed(timeout).getOrElse {
      sys.error(s"failed to start in $timeout")
    }

    val release1 = () => {
      val timeout = config.stopTimeout
      release.unsafeRunTimed(timeout).getOrElse {
        log.error(s"failed to release in $timeout")
      }
    }

    val toFuture = new (IO ~> Future) {
      def apply[A](fa: IO[A]) = ToFuture[IO].apply(fa)
    }

    val adapter1 = adapter.mapK(toFuture)
    
    (adapter1, release1)
  }


  override def preStart(): Unit = {
    super.preStart()
    val _ = adapter
  }

  override def postStop(): Unit = {
    try release() catch {
      case NonFatal(failure) => log.error(s"release failed with $failure", failure)
    }
    super.postStop()
  }


  def toKey: IO[ToKey] = ToKey(config).pure[IO]

  def kafkaJournalConfig(): KafkaJournalConfig = KafkaJournalConfig(config)

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

  def adapterOf(
    toKey: ToKey,
    origin: Option[Origin],
    serializer: EventSerializer[cats.Id],
    config: KafkaJournalConfig,
    metrics: JournalAdapter.Metrics[IO],
    metadataAndHeadersOf: MetadataAndHeadersOf[IO],
    batching: Batching[IO]): Resource[IO, JournalAdapter[IO]] = {

    val log1 = Log[IO](log)
    JournalAdapter.of[IO](
      toKey = toKey,
      origin = origin,
      serializer = serializer,
      config = config,
      metrics = metrics,
      log = log1,
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