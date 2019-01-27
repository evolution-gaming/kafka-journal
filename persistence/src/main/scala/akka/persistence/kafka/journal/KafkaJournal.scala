package akka.persistence.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}
import cats.{Parallel, ~>}
import cats.effect.{ContextShift, IO, Resource, Timer}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.util.{FromFuture, Par, Runtime}
import com.evolutiongaming.safeakka.actor.ActorLog
import com.typesafe.config.Config

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try
import scala.util.control.NonFatal

class KafkaJournal(config: Config) extends AsyncWriteJournal {

  implicit val system: ActorSystem = context.system
  implicit val ec: ExecutionContextExecutor = context.dispatcher
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)
  implicit val fromFuture: FromFuture[IO] = FromFuture.lift[IO]
  implicit val parallel: Parallel[IO, IO.Par] = IO.ioParallel(cs)
  implicit val par: Par[IO] = Par.liftIO(parallel)
  implicit val logOf: LogOf[IO] = LogOf[IO](system)
  implicit val runtime: Runtime[IO] = Runtime.lift[IO]

  val log: ActorLog = ActorLog(system, classOf[KafkaJournal])

  lazy val (adapter, release): (JournalAdapter[Future], () => Unit) = {
    val config = kafkaJournalConfig()

    log.debug(s"Config: $config")

    val resource = adapterOf(
      toKey(),
      origin(),
      serializer(),
      config,
      metrics())

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
      def apply[A](fa: IO[A]) = fa.unsafeToFuture()
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


  def toKey(): ToKey = ToKey(config)

  def kafkaJournalConfig(): KafkaJournalConfig = KafkaJournalConfig(config)

  def origin(): Option[Origin] = Some {
    Origin.HostName orElse Origin.AkkaHost(system) getOrElse Origin.AkkaName(system)
  }

  def serializer(): EventSerializer = EventSerializer(system)

  def metrics(): JournalAdapter.Metrics[IO] = JournalAdapter.Metrics.empty[IO]

  def adapterOf(
    toKey: ToKey,
    origin: Option[Origin],
    serializer: EventSerializer,
    config: KafkaJournalConfig,
    metrics: JournalAdapter.Metrics[IO]): Resource[IO, JournalAdapter[IO]] = {

    JournalAdapter.of[IO](toKey, origin, serializer, config, metrics, log)
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