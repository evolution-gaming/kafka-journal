package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.data.{NonEmptyList => Nel}
import cats.effect.concurrent.Ref
import cats.effect.implicits._
import cats.effect.{Concurrent, Resource, Sync, Timer}
import cats.syntax.all._
import cats.{Monad, Parallel}
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.catshelper.{LogOf, MeasureDuration, ToTry}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.SegmentNr.implicits._
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import java.time.Instant
import scala.annotation.tailrec


object ReplicatedCassandra {

  private sealed abstract class Main

  def of[
    F[_]
    : Concurrent: Parallel: Timer
    : ToTry: LogOf: Fail
    : CassandraCluster: CassandraSession
    : MeasureDuration
    : JsonCodec.Encode
  ](
    config: EventualCassandraConfig,
    origin: Option[Origin],
    metrics: Option[ReplicatedJournal.Metrics[F]],
  ): F[ReplicatedJournal[F]] = {

    for {
      schema        <- SetupSchema[F](config.schema, origin, config.consistencyConfig)
      statements    <- Statements.of[F](schema, config.consistencyConfig)
      log           <- LogOf[F].apply(ReplicatedCassandra.getClass)
      expiryService <- ExpiryService.of[F]
      _             <- log.info(s"kafka-journal version: ${Version.current.value}")
    } yield {
      val segmentOf = SegmentNrsOf[F](first = Segments.default, second = Segments.old)
      val journal = apply[F](config.segmentSize, segmentOf, statements, expiryService)
        .withLog(log)
      metrics
        .fold(journal) { metrics => journal.withMetrics(metrics) }
        .enhanceError
    }
  }

  def apply[F[_]: Sync: Parallel: Fail](
    segmentSizeDefault: SegmentSize,
    segmentNrsOf: SegmentNrsOf[F],
    statements: Statements[F],
    expiryService: ExpiryService[F],
  ): ReplicatedJournal[F] = {

    new Main with ReplicatedJournal[F] {

      def topics = {
        statements
          .selectTopics2()
          .parProduct(statements.selectTopics())
          .map { case (a, b) => a ++ b }
      }

      def journal(topic: Topic) = {
        class Main
        val result = new Main with ReplicatedTopicJournal[F] {

          def apply(partition: Partition) = {
            Ref[F]
              .of(false)
              .map { fixedRef =>
                new Main with ReplicatedPartitionJournal[F] {


                  def offsets = {
                    new Main with ReplicatedPartitionJournal.Offsets[F] {

                      def get = {
                        for {
                          offset <- statements.selectOffset2(topic, partition)
                          offset <- offset.fold { statements.selectOffset(topic, partition) } { _.some.pure[F] }
                        } yield offset
                      }

                      def create(offset: Offset, timestamp: Instant) = {
                        for {
                          a <- statements.insertPointer2(topic, partition, offset, timestamp, timestamp)
                          b <- statements.insertPointer(topic, partition, offset, timestamp, timestamp)
                          _ <- fixedRef.set(true)
                        } yield {
                          a.combine(b)
                        }
                      }

                      def update(offset: Offset, timestamp: Instant) = {
                        statements
                          .updatePointer(topic, partition, offset, timestamp)
                          .parProduct {
                            for {
                              fixed  <- fixedRef.get
                              result <- if (fixed) {
                                statements.updatePointer2(topic, partition, offset, timestamp)
                              } else {
                                for {
                                  pointer <- statements.selectPointer2(topic, partition)
                                  created = statements
                                    .selectPointer(topic, partition)
                                    .map { pointer =>
                                      pointer
                                        .flatMap { _.created }
                                        .getOrElse(timestamp)
                                    }
                                  result <- pointer.fold {
                                    for {
                                      created <- created
                                      result <- statements.insertPointer2(topic, partition, offset, created, timestamp)
                                    } yield result
                                  } { pointer =>
                                    pointer
                                      .created
                                      .fold {
                                        for {
                                          created <- created
                                          result <- statements.updatePointerCreated2(topic, partition, offset, created, timestamp)
                                        } yield result
                                      } { _ =>
                                        ().pure[F]
                                      }
                                  }
                                  _ <- fixedRef.set(true)
                                } yield result
                              }
                            } yield result
                          }
                          .map { case (a, b) => a.combine(b) }
                      }
                    }
                  }

                  def journal(id: String) = {

                    val key = Key(id = id, topic = topic)

                    def journalHeadRef = {

                      def head(segmentNr: SegmentNr) = {
                        statements
                          .metaJournal(key, segmentNr)
                          .journalHead
                          .toOptionT
                          .map { a => (a, segmentNr) }
                      }

                      for {
                        segmentNrs <- segmentNrsOf(key)
                        result     <- head(segmentNrs.first)
                          .orElse {
                            segmentNrs
                              .second
                              .toOptionT[F]
                              .flatMap { segmentNr => head(segmentNr) }
                          }
                          .value
                        (head, segmentNr) = result match {
                          case Some((head, segmentNr)) => (head.some, segmentNr)
                          case None                    => (none[JournalHead], segmentNrs.first)
                        }
                        ref <- Ref[F].of(head)
                      } yield {
                        (ref, segmentNr)
                      }
                    }

                    journalHeadRef
                      .map { case (journalHeadRef, segmentNr) =>

                        def metaJournal = statements.metaJournal(key, segmentNr)

                        class Main
                        new Main with ReplicatedKeyJournal[F] {

                          def append(
                            offset: Offset,
                            timestamp: Instant,
                            expireAfter: Option[ExpireAfter],
                            events: Nel[EventRecord[EventualPayloadAndType]]
                          ) = {

                            def partitionOffset = PartitionOffset(partition, offset)

                            def append(segmentSize: SegmentSize, offset: Option[Offset]) = {

                              @tailrec
                              def loop(
                                events: List[EventRecord[EventualPayloadAndType]],
                                s: Option[(Segment, Nel[EventRecord[EventualPayloadAndType]])],
                                result: F[Unit]
                              ): F[Unit] = {

                                def insert(segment: Segment, events: Nel[EventRecord[EventualPayloadAndType]]) = {
                                  val next = statements.insertRecords(key, segment.nr, events)
                                  result *> next
                                }

                                events match {
                                  case head :: tail =>
                                    val seqNr = head.event.seqNr
                                    s match {
                                      case Some((segment, batch)) => segment.next(seqNr) match {
                                        case None       => loop(tail, (segment, head :: batch).some, result)
                                        case Some(next) => loop(tail, (next, Nel.of(head)).some, insert(segment, batch))
                                      }
                                      case None                   => loop(tail, (Segment(seqNr, segmentSize), Nel.of(head)).some, result)
                                    }

                                  case Nil => s.fold(result) { case (segment, batch) => insert(segment, batch) }
                                }
                              }

                              val events1 = offset.fold {
                                events.toList
                              } { offset =>
                                events.filter { event => event.partitionOffset.offset > offset }
                              }
                              loop(events1, None, ().pure[F])
                            }

                            def appendAndSave(journalHead: Option[JournalHead]) = {

                              def appendAndSave = {
                                val seqNrLast = events.last.seqNr

                                val saveAndJournalHead = journalHead.fold {
                                  val deleteTo = events
                                    .head
                                    .seqNr
                                    .prev[Option]
                                    .map { _.toDeleteTo }

                                  expireAfter
                                    .traverse { expireAfter =>
                                      expiryService
                                        .expireOn(expireAfter, timestamp)
                                        .map { expireOn => Expiry(expireAfter, expireOn) }
                                    }
                                    .map { expiry =>
                                      val journalHead = JournalHead(
                                        partitionOffset = partitionOffset,
                                        segmentSize = segmentSizeDefault,
                                        seqNr = seqNrLast,
                                        deleteTo = deleteTo,
                                        expiry = expiry)
                                      val origin = events.head.origin
                                      val insert = metaJournal.insert(timestamp, journalHead, origin)
                                      (insert, journalHead)
                                    }
                                } { journalHead =>

                                  def updateOf = metaJournal.update(partitionOffset, timestamp)

                                  expiryService
                                    .action(journalHead.expiry, expireAfter, timestamp)
                                    .map { action =>
                                      val (expiry, update) = action match {
                                        case ExpiryService.Action.Update(expiry) =>
                                          (expiry.some, updateOf(seqNrLast, expiry))

                                        case ExpiryService.Action.Ignore =>
                                          (journalHead.expiry, updateOf(seqNrLast))

                                        case ExpiryService.Action.Remove =>
                                          val update = for {
                                            _ <- updateOf(seqNrLast)
                                            _ <- metaJournal.deleteExpiry
                                          } yield {}
                                          (none[Expiry], update)
                                      }
                                      val journalHead1 = journalHead.copy(
                                        partitionOffset = partitionOffset,
                                        seqNr = seqNrLast,
                                        expiry = expiry)
                                      (update, journalHead1)
                                    }
                                }

                                val offset = journalHead.map { _.partitionOffset.offset }

                                val result = for {
                                  saveAndJournalHead <- saveAndJournalHead
                                  (save, journalHead) = saveAndJournalHead
                                  _ <- append(journalHead.segmentSize, offset)
                                  _ <- save
                                } yield {
                                  journalHead.some
                                }
                                result.uncancelable
                              }

                              journalHead.fold {
                                appendAndSave
                              } { journalHead =>
                                if (offset <= journalHead.partitionOffset.offset) {
                                  none[JournalHead].pure[F]
                                } else {
                                  appendAndSave
                                }
                              }
                            }

                            for {
                              journalHead <- journalHeadRef.get
                              journalHead <- appendAndSave(journalHead)
                              result <- journalHead match {
                                case Some(journalHead) =>
                                  journalHeadRef
                                    .set(journalHead.some)
                                    .as(true)
                                case None              =>
                                  false.pure[F]
                              }
                            } yield result
                          }

                          def delete(
                            offset: Offset,
                            timestamp: Instant,
                            deleteTo: DeleteTo,
                            origin: Option[Origin]
                          ) = {

                            def partitionOffset = PartitionOffset(partition, offset)

                            val result = for {
                              journalHead <- journalHeadRef.get
                              journalHead <- journalHead.fold {
                                val journalHead = JournalHead(
                                  partitionOffset = partitionOffset,
                                  segmentSize = segmentSizeDefault,
                                  seqNr = deleteTo.value,
                                  deleteTo = deleteTo.some)
                                metaJournal
                                  .insert(timestamp, journalHead, origin)
                                  .as(journalHead.some)
                              } { journalHead =>
                                if (offset > journalHead.partitionOffset.offset) {
                                  val deleteTo0 = journalHead.deleteTo
                                  val seqNr = journalHead.seqNr
                                  val seqNr1 = deleteTo0.fold { seqNr } { _.value.max(seqNr) }
                                  val deleteTo1 = deleteTo
                                    .value
                                    .min(seqNr1)
                                    .toDeleteTo
                                  for {
                                    _ <- {
                                      val update = metaJournal.update(partitionOffset, timestamp)
                                      if (seqNr1 == seqNr) {
                                        if (deleteTo0.contains(deleteTo1)) {
                                          update()
                                        } else {
                                          update(deleteTo1)
                                        }
                                      } else {
                                        if (deleteTo0.contains(deleteTo1)) {
                                          update(seqNr1)
                                        } else {
                                          update(seqNr1, deleteTo1)
                                        }
                                      }
                                    }
                                    _ <- deleteTo0
                                      .fold { SeqNr.min.some } { _.value.next[Option] }
                                      .foldMapM { from =>
                                        val to = deleteTo.value.min(seqNr)
                                        if (from <= to) {
                                          val segmentSize = journalHead.segmentSize
                                          for {
                                            segmentNrs <- from
                                              .toSegmentNr(segmentSize)
                                              .to[F] { to.toSegmentNr(segmentSize) }
                                            result <- {
                                              if (to >= seqNr) {
                                                segmentNrs.parFoldMapA { segmentNr =>
                                                  statements
                                                    .deleteRecords(key, segmentNr)
                                                    .uncancelable
                                                }
                                              } else {
                                                @tailrec def loop(as: List[SegmentNr], result: List[F[Unit]]): List[F[Unit]] = {
                                                  as match {
                                                    case Nil      => result
                                                    case a :: Nil => statements.deleteRecordsTo(key, a, to) :: result
                                                    case a :: as  => loop(as, statements.deleteRecords(key, a) :: result)
                                                  }
                                                }

                                                loop(segmentNrs, List.empty).parFoldMap1 { _.uncancelable }
                                              }
                                            }
                                          } yield result
                                        } else {
                                          ().pure[F]
                                        }
                                      }
                                  } yield {
                                    journalHead
                                      .copy(
                                        partitionOffset = partitionOffset,
                                        seqNr = seqNr1,
                                        deleteTo = deleteTo1.some)
                                      .some
                                  }
                                } else {
                                  none[JournalHead].pure[F]
                                }
                              }
                              result <- journalHead.fold {
                                false.pure[F]
                              } { journalHead =>
                                journalHeadRef
                                  .set(journalHead.some)
                                  .as(true)
                              }
                            } yield result
                            result.uncancelable
                          }

                          def purge(
                            offset: Offset,
                            timestamp: Instant
                          ) = {
                            for {
                              journalHead <- journalHeadRef.get
                              result      <- journalHead.fold {
                                false.pure[F]
                              } { journalHead =>
                                if (offset >= journalHead.partitionOffset.offset) {
                                  val segmentSize = journalHead.segmentSize
                                  val seqNr = journalHead.seqNr
                                  val update = metaJournal.update(journalHead.partitionOffset, timestamp)
                                  val result = for {
                                    result     <- journalHead
                                      .deleteTo
                                      .fold {
                                        val deleteTo = seqNr.toDeleteTo
                                        for {
                                          _ <- update(deleteTo)
                                          _ <- journalHeadRef.set {
                                            journalHead
                                              .copy(deleteTo = deleteTo.some)
                                              .some
                                          }
                                        } yield {
                                          (SeqNr.min, seqNr)
                                        }
                                      } { deleteTo =>
                                        val seqNr = journalHead.seqNr
                                        if (deleteTo.value < seqNr) {
                                          val deleteTo1 = seqNr.toDeleteTo
                                          for {
                                            _ <- update(deleteTo1)
                                            _ <- journalHeadRef.set {
                                              journalHead
                                                .copy(deleteTo = deleteTo1.some)
                                                .some
                                            }
                                          } yield {
                                            (deleteTo.value, seqNr)
                                          }
                                        } else if (deleteTo.value == seqNr) {
                                          (seqNr, seqNr).pure[F]
                                        } else {
                                          val seqNr1 = deleteTo.value
                                          for {
                                            _ <- update(seqNr1)
                                            _ <- journalHeadRef.set {
                                              journalHead
                                                .copy(seqNr = seqNr1)
                                                .some
                                            }
                                          } yield {
                                            (seqNr, seqNr1)
                                          }
                                        }
                                      }
                                    (from, to)  = result
                                    segmentNrs <- from
                                      .prev[Option]
                                      .getOrElse { from }
                                      .toSegmentNr(segmentSize)
                                      .to[F] {
                                        to
                                          .next[Option]
                                          .getOrElse { journalHead.seqNr }
                                          .toSegmentNr(segmentSize)
                                      }
                                    _          <- segmentNrs.parFoldMapA { segmentNr =>
                                      statements
                                        .deleteRecords(key, segmentNr)
                                        .uncancelable
                                    }
                                    _          <- metaJournal.delete
                                    _          <- journalHeadRef.set(none)
                                  } yield true
                                  result.uncancelable
                                } else {
                                  false.pure[F]
                                }
                              }
                            } yield result
                          }
                        }
                      }
                      .toResource
                  }
                }
              }
              .toResource
          }
        }
        result.pure[Resource[F, *]]
      }
    }
  }


  trait MetaJournalStatements[F[_]] {
    import MetaJournalStatements._

    def apply(key: Key, segment: SegmentNr): ByKey[F]
  }

  object MetaJournalStatements {

    def of[F[_]: Monad: CassandraSession](
      schema: Schema,
      consistencyConfig: ConsistencyConfig
    ): F[MetaJournalStatements[F]] = {
      of[F](schema.metaJournal, consistencyConfig)
    }


    def of[F[_]: Monad: CassandraSession](
      metaJournal: TableName,
      consistencyConfig: ConsistencyConfig
    ): F[MetaJournalStatements[F]] = {

      for {
        selectJournalHead     <- cassandra.MetaJournalStatements.SelectJournalHead.of[F](metaJournal, consistencyConfig.read)
        insert                <- cassandra.MetaJournalStatements.Insert.of[F](metaJournal, consistencyConfig.write)
        update                <- cassandra.MetaJournalStatements.Update.of[F](metaJournal, consistencyConfig.write)
        updateSeqNr           <- cassandra.MetaJournalStatements.UpdateSeqNr.of[F](metaJournal, consistencyConfig.write)
        updateExpiry          <- cassandra.MetaJournalStatements.UpdateExpiry.of[F](metaJournal, consistencyConfig.write)
        updateDeleteTo        <- cassandra.MetaJournalStatements.UpdateDeleteTo.of[F](metaJournal, consistencyConfig.write)
        updatePartitionOffset <- cassandra.MetaJournalStatements.UpdatePartitionOffset.of[F](metaJournal, consistencyConfig.write)
        delete                <- cassandra.MetaJournalStatements.Delete.of[F](metaJournal, consistencyConfig.write)
        deleteExpiry          <- cassandra.MetaJournalStatements.DeleteExpiry.of[F](metaJournal, consistencyConfig.write)
      } yield {
        apply(
          selectJournalHead,
          insert,
          update,
          updateSeqNr,
          updateExpiry,
          updateDeleteTo,
          updatePartitionOffset,
          delete,
          deleteExpiry)
      }
    }

    private sealed abstract class MetaJournal

    def apply[F[_]](
      selectJournalHead: cassandra.MetaJournalStatements.SelectJournalHead[F],
      insert: cassandra.MetaJournalStatements.Insert[F],
      update: cassandra.MetaJournalStatements.Update[F],
      updateSeqNr: cassandra.MetaJournalStatements.UpdateSeqNr[F],
      updateExpiry: cassandra.MetaJournalStatements.UpdateExpiry[F],
      updateDeleteTo: cassandra.MetaJournalStatements.UpdateDeleteTo[F],
      updatePartitionOffset: cassandra.MetaJournalStatements.UpdatePartitionOffset[F],
      delete: cassandra.MetaJournalStatements.Delete[F],
      deleteExpiry: cassandra.MetaJournalStatements.DeleteExpiry[F]
    ): MetaJournalStatements[F] = {

      val inset1 = insert
      val update1 = update
      val delete1 = delete
      val deleteExpiry1 = deleteExpiry

      new MetaJournal with MetaJournalStatements[F] {

        def apply(key: Key, segment: SegmentNr) = {
          new MetaJournal with ByKey[F] {

            def journalHead = selectJournalHead(key, segment)

            def insert(timestamp: Instant, journalHead: JournalHead, origin: Option[Origin]) = {
              inset1(key, segment, timestamp, timestamp, journalHead, origin)
            }

            def update(partitionOffset: PartitionOffset, timestamp: Instant) = {
              new MetaJournal with ByKey.Update[F] {

                def apply() = {
                  updatePartitionOffset(key, segment, partitionOffset, timestamp)
                }

                def apply(seqNr: SeqNr) = {
                  updateSeqNr(key, segment, partitionOffset, timestamp, seqNr)
                }

                def apply(seqNr: SeqNr, expiry: Expiry) = {
                  updateExpiry(key, segment, partitionOffset, timestamp, seqNr, expiry)
                }

                def apply(deleteTo: DeleteTo) = {
                  updateDeleteTo(key, segment, partitionOffset, timestamp, deleteTo)
                }

                def apply(seqNr: SeqNr, deleteTo: DeleteTo) = {
                  update1(key, segment, partitionOffset, timestamp, seqNr, deleteTo)
                }
              }
            }

            def delete = delete1(key, segment)

            def deleteExpiry = deleteExpiry1(key, segment)
          }
        }
      }
    }


    trait ByKey[F[_]] {
      import ByKey._

      def journalHead: F[Option[JournalHead]]

      def insert(timestamp: Instant, journalHead: JournalHead, origin: Option[Origin]): F[Unit]

      def update(partitionOffset: PartitionOffset, timestamp: Instant): Update[F]

      def delete: F[Unit]

      def deleteExpiry: F[Unit]
    }


    object ByKey {
      trait Update[F[_]] {

        def apply(): F[Unit]

        def apply(seqNr: SeqNr): F[Unit]

        def apply(seqNr: SeqNr, expiry: Expiry): F[Unit]

        def apply(deleteTo: DeleteTo): F[Unit]

        def apply(seqNr: SeqNr, deleteTo: DeleteTo): F[Unit]
      }
    }
  }


  final case class Statements[F[_]](
    insertRecords: JournalStatements.InsertRecords[F],
    deleteRecordsTo: JournalStatements.DeleteTo[F],
    deleteRecords: JournalStatements.Delete[F],
    metaJournal: MetaJournalStatements[F],
    selectOffset: PointerStatements.SelectOffset[F],
    selectOffset2: Pointer2Statements.SelectOffset[F],
    selectPointer: PointerStatements.Select[F],
    selectPointer2: Pointer2Statements.Select[F],
    insertPointer: PointerStatements.Insert[F],
    insertPointer2: Pointer2Statements.Insert[F],
    updatePointer: PointerStatements.Update[F],
    updatePointer2: Pointer2Statements.Update[F],
    updatePointerCreated2: Pointer2Statements.UpdateCreated[F],
    selectTopics: PointerStatements.SelectTopics[F],
    selectTopics2: Pointer2Statements.SelectTopics[F])

  object Statements {

    def apply[F[_]](implicit F: Statements[F]): Statements[F] = F

    def of[F[_]: Monad: CassandraSession: ToTry: JsonCodec.Encode](
      schema: Schema,
      consistencyConfig: ConsistencyConfig
    ): F[Statements[F]] = {
      for {
        insertRecords          <- JournalStatements.InsertRecords.of[F](schema.journal, consistencyConfig.write)
        deleteRecordsTo        <- JournalStatements.DeleteTo.of[F](schema.journal, consistencyConfig.write)
        deleteRecords          <- JournalStatements.Delete.of[F](schema.journal, consistencyConfig.write)
        metaJournal            <- MetaJournalStatements.of[F](schema, consistencyConfig)
        selectOffset           <- PointerStatements.SelectOffset.of[F](schema.pointer, consistencyConfig.read)
        selectOffset2          <- Pointer2Statements.SelectOffset.of[F](schema.pointer2, consistencyConfig.read)
        selectPointer          <- PointerStatements.Select.of[F](schema.pointer, consistencyConfig.read)
        selectPointer2         <- Pointer2Statements.Select.of[F](schema.pointer2, consistencyConfig.read)
        insertPointer          <- PointerStatements.Insert.of[F](schema.pointer, consistencyConfig.write)
        insertPointer2         <- Pointer2Statements.Insert.of[F](schema.pointer2, consistencyConfig.write)
        updatePointer          <- PointerStatements.Update.of[F](schema.pointer, consistencyConfig.write)
        updatePointer2         <- Pointer2Statements.Update.of[F](schema.pointer2, consistencyConfig.write)
        updatePointerCreated2  <- Pointer2Statements.UpdateCreated.of[F](schema.pointer2, consistencyConfig.write)
        selectTopics           <- PointerStatements.SelectTopics.of[F](schema.pointer, consistencyConfig.read)
        selectTopics2          <- Pointer2Statements.SelectTopics.of[F](schema.pointer2, consistencyConfig.read)
      } yield {
        Statements(
          insertRecords,
          deleteRecordsTo,
          deleteRecords,
          metaJournal,
          selectOffset,
          selectOffset2,
          selectPointer,
          selectPointer2,
          insertPointer,
          insertPointer2,
          updatePointer,
          updatePointer2,
          updatePointerCreated2,
          selectTopics,
          selectTopics2,
        )
      }
    }
  }
}
