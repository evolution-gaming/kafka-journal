package akka.persistence.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import cats.effect._
import cats.implicits._
import cats.{Parallel, ~>}
import com.evolutiongaming.catshelper.{FromFuture, Log, LogOf, ToFuture}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.smetrics.MeasureDuration
import com.typesafe.config.Config

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try

class KafkaJournal(config: Config) extends AsyncWriteJournal {

  implicit val system: ActorSystem                = context.system
  implicit val executor: ExecutionContextExecutor = context.dispatcher

  implicit val contextShift    : ContextShift[IO]     = IO.contextShift(executor)
  implicit val parallel        : Parallel[IO, IO.Par] = IO.ioParallel(contextShift)
  implicit val timer           : Timer[IO]            = IO.timer(executor)
  implicit val logOf           : LogOf[IO]            = LogOfFromAkka[IO](system)
  implicit val randomId        : RandomId[IO]         = RandomId.uuid[IO]
  implicit val measureDuration : MeasureDuration[IO]  = MeasureDuration.fromClock(Clock[IO])

  lazy val (adapter, release): (JournalAdapter[Future], () => Unit) = {
    
    val (adapter, release) = adapterIO.allocated.unsafeRunSync()

    val toFuture = new (IO ~> Future) {
      def apply[A](fa: IO[A]) = ToFuture[IO].apply(fa)
    }

    val fromFuture = new (Future ~> IO) {
      def apply[A](fa: Future[A]) = FromFuture[IO].apply(fa)
    }

    val adapter1 = adapter.mapK(toFuture, fromFuture)
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

  def toKey: Resource[IO, ToKey] = {
    val toKey = ToKey(config)
    Resource.liftF(toKey.pure[IO])
  }

  def kafkaJournalConfig: IO[KafkaJournalConfig] = KafkaJournalConfig(config).pure[IO]

  def origin: IO[Option[Origin]] = {
    Sync[IO].delay {
      val origin = Origin.HostName orElse Origin.AkkaHost(system) getOrElse Origin.AkkaName(system)
      origin.some
    }
  }

  def serializer: Resource[IO, EventSerializer[cats.Id]] = {
    val serializer = EventSerializer.unsafe(system)
    Resource.liftF(serializer.pure[IO])
  }

  def metrics: Resource[IO, JournalAdapter.Metrics[IO]] = {
    val metrics = JournalAdapter.Metrics.empty[IO]
    Resource.liftF(metrics.pure[IO])
  }

  def metadataAndHeadersOf: Resource[IO, MetadataAndHeadersOf[IO]] = {
    val metadataAndHeadersOf = MetadataAndHeadersOf.empty[IO]
    Resource.liftF(metadataAndHeadersOf.pure[IO])
  }

  def batching(config: KafkaJournalConfig): Resource[IO, Batching[IO]] = {
    val batching = Batching.byNumberOfEvents[IO](config.maxEventsInBatch)
    Resource.liftF(batching.pure[IO])
  }

  def cassandraClusterOf: Resource[IO, CassandraClusterOf[IO]] = {
    val cassandraClusterOf = CassandraClusterOf.of[IO]
    Resource.liftF(cassandraClusterOf)
  }

  def adapterIO: Resource[IO, JournalAdapter[IO]] = {
    for {
      config  <- Resource.liftF(kafkaJournalConfig)
      adapter <- adapterIO(config)
    } yield adapter
  }

  def adapterIO(config: KafkaJournalConfig): Resource[IO, JournalAdapter[IO]] = {
    val resource = for {
      log                  <- Resource.liftF(logOf(classOf[KafkaJournal]))
      _                    <- Resource.liftF(log.debug(s"config: $config"))
      toKey                <- toKey
      origin               <- Resource.liftF(origin)
      metadataAndHeadersOf <- metadataAndHeadersOf
      serializer           <- serializer
      metrics              <- metrics
      batching             <- batching(config)
      cassandraClusterOf   <- cassandraClusterOf
      adapter              <- adapterOf(
        toKey                = toKey,
        origin               = origin,
        serializer           = serializer,
        config               = config,
        metrics              = metrics,
        metadataAndHeadersOf = metadataAndHeadersOf,
        batching             = batching,
        log                  = log,
        cassandraClusterOf   = cassandraClusterOf)
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

  def adapterOf(
    toKey: ToKey,
    origin: Option[Origin],
    serializer: EventSerializer[cats.Id],
    config: KafkaJournalConfig,
    metrics: JournalAdapter.Metrics[IO],
    metadataAndHeadersOf: MetadataAndHeadersOf[IO],
    batching: Batching[IO],
    log: Log[IO],
    cassandraClusterOf: CassandraClusterOf[IO]
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
      cassandraClusterOf = cassandraClusterOf)
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

  def asyncReplayMessages(
    persistenceId: PersistenceId,
    from: Long,
    to: Long,
    max: Long)(
    f: PersistentRepr => Unit
  ): Future[Unit] = {

    val seqNrFrom = SeqNr(from, SeqNr.Min)
    val seqNrTo = SeqNr(to, SeqNr.Max)
    val range = SeqRange(seqNrFrom, seqNrTo)
    val f1 = (a: PersistentRepr) => Future.fromTry(Try { f(a) })
    adapter.replay(persistenceId, range, max)(f1)
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