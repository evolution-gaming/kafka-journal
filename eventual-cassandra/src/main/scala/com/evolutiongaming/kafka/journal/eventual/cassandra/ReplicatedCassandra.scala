package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.data.NonEmptyList as Nel
import cats.effect.std.UUIDGen
import cats.effect.syntax.all.*
import cats.effect.{Async, Ref, Resource, Sync}
import cats.syntax.all.*
import cats.{Monad, Parallel}
import com.evolutiongaming.catshelper.ParallelHelper.*
import com.evolutiongaming.catshelper.{LogOf, MeasureDuration, ToTry}
import com.evolutiongaming.kafka.journal.cassandra.CassandraConsistencyConfig
import com.evolutiongaming.kafka.journal.eventual.*
import com.evolutiongaming.kafka.journal.eventual.ReplicatedKeyJournal.Changed
import com.evolutiongaming.kafka.journal.eventual.cassandra.JournalStatements.JournalRecord
import com.evolutiongaming.kafka.journal.eventual.cassandra.ReplicatedCassandra.MetaJournalStatements.ByKey
import com.evolutiongaming.kafka.journal.eventual.cassandra.SegmentNr.implicits.*
import com.evolutiongaming.kafka.journal.util.CatsHelper.*
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.{cassandra as _, *}
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import java.time.Instant
import scala.annotation.tailrec
import scala.collection.immutable.SortedSet

private[journal] object ReplicatedCassandra {

  private sealed abstract class Main

  def of[
    F[_]: Async: Parallel: ToTry: LogOf: Fail: CassandraCluster: CassandraSession: MeasureDuration: JsonCodec.Encode,
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
      val journal   = apply1[F](config.segmentSize, segmentOf, statements, expiryService).withLog(log)
      metrics
        .fold(journal) { metrics => journal.withMetrics(metrics) }
        .enhanceError
    }
  }

  @deprecated("use `apply1` instead", "3.6.0")
  def apply[F[_]: Sync: Parallel: Fail](
    segmentSizeDefault: SegmentSize,
    segmentNrsOf: SegmentNrsOf[F],
    statements: Statements[F],
    expiryService: ExpiryService[F],
  ): ReplicatedJournal[F] = {
    apply1(segmentSizeDefault, segmentNrsOf, statements, expiryService)
  }

  def apply1[F[_]: Sync: Parallel: Fail: UUIDGen](
    segmentSizeDefault: SegmentSize,
    segmentNrsOf: SegmentNrsOf[F],
    statements: Statements[F],
    expiryService: ExpiryService[F],
  ): ReplicatedJournal[F] = {

    new Main with ReplicatedJournal[F] {

      def topics: F[SortedSet[Topic]] = {
        statements.selectTopics2()
      }

      def journal(topic: Topic): Resource[F, ReplicatedTopicJournal[F]] = {
        class Main
        val result = new Main with ReplicatedTopicJournal[F] {

          def apply(partition: Partition): Resource[F, ReplicatedPartitionJournal[F]] = {
            new Main with ReplicatedPartitionJournal[F] {

              def offsets: ReplicatedPartitionJournal.Offsets[F] = {
                new Main with ReplicatedPartitionJournal.Offsets[F] {

                  def get: F[Option[Offset]] = {
                    statements.selectOffset2(topic, partition)
                  }

                  def create(offset: Offset, timestamp: Instant): F[Unit] = {
                    statements
                      .insertPointer2(topic, partition, offset, timestamp, timestamp)
                      .parProduct(statements.insertPointer(topic, partition, offset, timestamp, timestamp))
                      .map { case (a, b) => a.combine(b) }
                  }

                  def update(offset: Offset, timestamp: Instant): F[Unit] = {
                    statements
                      .updatePointer(topic, partition, offset, timestamp)
                      .parProduct(statements.updatePointer2(topic, partition, offset, timestamp))
                      .map { case (a, b) => a.combine(b) }
                  }
                }
              }

              def journal(id: String): Resource[F, ReplicatedKeyJournal[F]] = {

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
                    result <- head(segmentNrs.first).orElse {
                      segmentNrs
                        .second
                        .toOptionT[F]
                        .flatMap { segmentNr => head(segmentNr) }
                    }.value
                    (head, segmentNr) = result match {
                      case Some((head, segmentNr)) => (head.some, segmentNr)
                      case None                    => (none[JournalHead], segmentNrs.first)
                    }
                    ref <- Ref[F].of(head)
                  } yield {
                    (ref, segmentNr)
                  }
                }

                journalHeadRef.map {
                  case (journalHeadRef, segmentNr) =>
                    def metaJournal = statements.metaJournal(key, segmentNr)

                    class Main
                    new Main with ReplicatedKeyJournal[F] {

                      def append(
                        offset: Offset,
                        timestamp: Instant,
                        expireAfter: Option[ExpireAfter],
                        events: Nel[EventRecord[EventualPayloadAndType]],
                      ): F[Changed] = {

                        def partitionOffset = PartitionOffset(partition, offset)

                        def append(journalHead: JournalHead, offset: Option[Offset]) = {

                          @tailrec
                          def loop(
                            events: List[EventRecord[EventualPayloadAndType]],
                            s: Option[(Segment, Nel[EventRecord[EventualPayloadAndType]])],
                            result: F[Unit],
                          ): F[Unit] = {

                            def insert(segment: Segment, events: Nel[EventRecord[EventualPayloadAndType]]) = {
                              val records = events.map { event => JournalRecord(event, journalHead.recordId) }
                              val next    = statements.insertRecords(key, segment.nr, records)
                              result *> next
                            }

                            events match {
                              case head :: tail =>
                                val seqNr = head.event.seqNr
                                s match {
                                  case Some((segment, batch)) =>
                                    segment.next(seqNr) match {
                                      case None       => loop(tail, (segment, head :: batch).some, result)
                                      case Some(next) => loop(tail, (next, Nel.of(head)).some, insert(segment, batch))
                                    }
                                  case None => loop(tail, (Segment(seqNr, journalHead.segmentSize), Nel.of(head)).some, result)
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
                                .product(RecordId.of[F])
                                .map {
                                  case (expiry, recordId) =>
                                    val journalHead = JournalHead(
                                      partitionOffset = partitionOffset,
                                      segmentSize     = segmentSizeDefault,
                                      seqNr           = seqNrLast,
                                      deleteTo        = deleteTo,
                                      expiry          = expiry,
                                      recordId        = recordId.some,
                                    )
                                    val origin = events.head.origin
                                    val insert = metaJournal.insert(timestamp, journalHead, origin)
                                    (insert, journalHead)
                                }
                            } { journalHead =>
                              def updateOf: ByKey.Update[F] = metaJournal.update(partitionOffset, timestamp)

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
                                  val journalHead1 =
                                    journalHead.copy(partitionOffset = partitionOffset, seqNr = seqNrLast, expiry = expiry)
                                  (update, journalHead1)
                                }
                            }

                            val offset = journalHead.map { _.partitionOffset.offset }

                            val result = for {
                              saveAndJournalHead <- saveAndJournalHead
                              (save, journalHead) = saveAndJournalHead
                              _                  <- append(journalHead, offset)
                              _                  <- save
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
                            case None =>
                              false.pure[F]
                          }
                        } yield result
                      }

                      def delete(
                        offset: Offset,
                        timestamp: Instant,
                        deleteTo: DeleteTo,
                        origin: Option[Origin],
                        setSeqNr: Option[SeqNr],
                      ): F[Changed] = {

                        def partitionOffset = PartitionOffset(partition, offset)

                        val result = for {
                          journalHead <- journalHeadRef.get
                          journalHead <- journalHead.fold {
                            val journalHead = JournalHead(
                              partitionOffset = partitionOffset,
                              segmentSize     = segmentSizeDefault,
                              seqNr           = deleteTo.value,
                              deleteTo        = deleteTo.some,
                            )
                            metaJournal
                              .insert(timestamp, journalHead, origin)
                              .as(journalHead.some)
                          } { journalHead =>
                            if (offset > journalHead.partitionOffset.offset) {
                              val headDeleteTo = journalHead.deleteTo
                              val headSeqNr    = journalHead.seqNr

                              // keep existing `seqNr` or update to higher one
                              // usually because `delete batch` removed previous `append batch`
                              val validatedSeqNr: SeqNr = setSeqNr.fold { headSeqNr } { _.max(headSeqNr) }

                              // clamp down to requested or max valid seqNr
                              // in default protocol we have `Journal#delete(DeleteTo.max)`
                              // `DeleteTo.max` can be used to discard all traces, but keep `seqNr`
                              val validatedDeleteTo = deleteTo
                                .value
                                .min(validatedSeqNr)
                                .toDeleteTo

                              val updateSeqNr = setSeqNr.isDefined
                              // different from value in head, must be less or equal to head's `seqNr`
                              val updateDeleteTo = headDeleteTo.forall(_ < validatedDeleteTo)

                              for {
                                _ <- {
                                  val update = metaJournal.update(partitionOffset, timestamp)

                                  if (updateSeqNr && updateDeleteTo) {
                                    // delete batch dropped previous append - we have to update `seqNr`
                                    update(validatedSeqNr, validatedDeleteTo)
                                  } else if (updateSeqNr) {
                                    update(validatedSeqNr)
                                  } else if (updateDeleteTo) {
                                    update(validatedDeleteTo)
                                  } else {
                                    // head already has requested `deleteTo` - update only `partition-offset` and `updated`
                                    update()
                                  }
                                }
                                _ <- headDeleteTo
                                  .fold { SeqNr.min.some } { _.value.next[Option] }
                                  .foldMapM { from =>
                                    val to = validatedDeleteTo.value.min(validatedSeqNr)
                                    if (from <= to) {
                                      val segmentSize = journalHead.segmentSize
                                      for {
                                        segmentNrs <- from
                                          .toSegmentNr(segmentSize)
                                          .to[F] { to.toSegmentNr(segmentSize) }
                                        result <- {
                                          if (to >= validatedSeqNr) {
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
                                    seqNr           = validatedSeqNr,
                                    deleteTo        = validatedDeleteTo.some,
                                  )
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
                        timestamp: Instant,
                      ): F[Changed] = {
                        for {
                          journalHead <- journalHeadRef.get
                          result <- journalHead.fold {
                            false.pure[F]
                          } { journalHead =>
                            if (offset >= journalHead.partitionOffset.offset) {
                              val segmentSize = journalHead.segmentSize
                              val headSeqNr   = journalHead.seqNr
                              val update      = metaJournal.update(journalHead.partitionOffset, timestamp)
                              val result = for {
                                result <- journalHead
                                  .deleteTo
                                  .fold {
                                    // empty `deleteTo` - set it to head's `seqNr`
                                    val deleteTo = headSeqNr.toDeleteTo
                                    for {
                                      _ <- update(deleteTo)
                                      _ <- journalHeadRef.set {
                                        journalHead
                                          .copy(deleteTo = deleteTo.some)
                                          .some
                                      }
                                    } yield {
                                      // journal has never been pruned, prune it fully from very beginning
                                      (SeqNr.min, headSeqNr)
                                    }
                                  } { deleteTo =>
                                    if (deleteTo.value < headSeqNr) {
                                      // some part of journal is not deleted, yet
                                      val deleteTo1 = headSeqNr.toDeleteTo
                                      for {
                                        _ <- update(deleteTo1)
                                        _ <- journalHeadRef.set {
                                          journalHead
                                            .copy(deleteTo = deleteTo1.some)
                                            .some
                                        }
                                      } yield {
                                        // prune yet non-deleted part of journal
                                        (deleteTo.value, headSeqNr)
                                      }
                                    } else if (deleteTo.value == headSeqNr) {
                                      // all is fully pruned - nothing to do
                                      (headSeqNr, headSeqNr).pure[F]
                                    } else {
                                      // somehow `deleteTo` is ahead of head's `seqNr`
                                      // TODO MR how this can happen? - in `def append` we clamp down `DeleteTo`
                                      val seqNr1 = deleteTo.value
                                      for {
                                        // advance head's `seqNr` to catch the `deleteTo` value
                                        _ <- update(seqNr1)
                                        _ <- journalHeadRef.set {
                                          journalHead
                                            .copy(seqNr = seqNr1)
                                            .some
                                        }
                                      } yield {
                                        // assume that journal has been pruned till actual `deleteTo`
                                        // and `seqNr` was not advanced for some reason
//                                        (headSeqNr, seqNr1) // TODO MR do we need to prune this part of journal?
                                        (seqNr1, seqNr1)
                                      }
                                    }
                                  }
                                (from, to) = result
                                segmentNrs <- from
                                  .prev[Option]
                                  .getOrElse { from }
                                  .toSegmentNr(segmentSize)
                                  .to[F] {
                                    to
                                      .next[Option]
                                      .getOrElse { headSeqNr }
                                      .toSegmentNr(segmentSize)
                                  }
                                _ <- Sync[F].whenA(from < to) {
                                  segmentNrs.parFoldMapA { segmentNr =>
                                    statements
                                      .deleteRecords(key, segmentNr)
                                      .uncancelable
                                  }
                                }
                                _ <- metaJournal.delete
                                _ <- journalHeadRef.set(none)
                              } yield true
                              result.uncancelable
                            } else {
                              false.pure[F]
                            }
                          }
                        } yield result
                      }
                    }
                }.toResource
              }
            }.pure[Resource[F, *]]
          }
        }
        result.pure[Resource[F, *]]
      }
    }
  }

  trait MetaJournalStatements[F[_]] {
    import MetaJournalStatements.*

    def apply(key: Key, segment: SegmentNr): ByKey[F]
  }

  object MetaJournalStatements {

    def of[F[_]: Monad: CassandraSession](
      schema: Schema,
      consistencyConfig: CassandraConsistencyConfig,
    ): F[MetaJournalStatements[F]] = {
      of[F](schema.metaJournal, consistencyConfig)
    }

    def of[F[_]: Monad: CassandraSession](
      metaJournal: TableName,
      consistencyConfig: CassandraConsistencyConfig,
    ): F[MetaJournalStatements[F]] = {

      for {
        selectJournalHead <- cassandra.MetaJournalStatements.SelectJournalHead.of[F](metaJournal, consistencyConfig.read)
        insert            <- cassandra.MetaJournalStatements.Insert.of[F](metaJournal, consistencyConfig.write)
        update            <- cassandra.MetaJournalStatements.Update.of[F](metaJournal, consistencyConfig.write)
        updateSeqNr       <- cassandra.MetaJournalStatements.UpdateSeqNr.of[F](metaJournal, consistencyConfig.write)
        updateExpiry      <- cassandra.MetaJournalStatements.UpdateExpiry.of[F](metaJournal, consistencyConfig.write)
        updateDeleteTo    <- cassandra.MetaJournalStatements.UpdateDeleteTo.of[F](metaJournal, consistencyConfig.write)
//        updateDeleteToAndSeqNr <- cassandra
//          .MetaJournalStatements
//          .UpdateDeleteToAndSeqNr
//          .of[F](metaJournal, consistencyConfig.write)
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
//          updateDeleteToAndSeqNr,
          updatePartitionOffset,
          delete,
          deleteExpiry,
        )
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
//      updateDeleteToAndSeqNr: cassandra.MetaJournalStatements.UpdateDeleteToAndSeqNr[F],
      updatePartitionOffset: cassandra.MetaJournalStatements.UpdatePartitionOffset[F],
      delete: cassandra.MetaJournalStatements.Delete[F],
      deleteExpiry: cassandra.MetaJournalStatements.DeleteExpiry[F],
    ): MetaJournalStatements[F] = {

      val inset1        = insert
      val update1       = update
      val delete1       = delete
      val deleteExpiry1 = deleteExpiry

      new MetaJournal with MetaJournalStatements[F] {

        def apply(key: Key, segment: SegmentNr): ByKey[F] = {
          new MetaJournal with ByKey[F] {

            def journalHead: F[Option[JournalHead]] = selectJournalHead(key, segment)

            def insert(timestamp: Instant, journalHead: JournalHead, origin: Option[Origin]): F[Unit] = {
              inset1(key, segment, timestamp, timestamp, journalHead, origin)
            }

            def update(partitionOffset: PartitionOffset, timestamp: Instant): ByKey.Update[F] = {
              new MetaJournal with ByKey.Update[F] {

                def apply(): F[Unit] = {
                  updatePartitionOffset(key, segment, partitionOffset, timestamp)
                }

                def apply(seqNr: SeqNr): F[Unit] = {
                  updateSeqNr(key, segment, partitionOffset, timestamp, seqNr)
                }

                def apply(seqNr: SeqNr, expiry: Expiry): F[Unit] = {
                  updateExpiry(key, segment, partitionOffset, timestamp, seqNr, expiry)
                }

                def apply(deleteTo: DeleteTo): F[Unit] = {
                  updateDeleteTo(key, segment, partitionOffset, timestamp, deleteTo)
                }

//                def apply(deleteTo: DeleteTo, seqNr: SeqNr): F[Unit] = {
//                  updateDeleteToAndSeqNr(key, segment, partitionOffset, timestamp, deleteTo, seqNr)
//                }

                def apply(seqNr: SeqNr, deleteTo: DeleteTo): F[Unit] = {
                  update1(key, segment, partitionOffset, timestamp, seqNr, deleteTo)
                }
              }
            }

            def delete: F[Unit] = delete1(key, segment)

            def deleteExpiry: F[Unit] = deleteExpiry1(key, segment)
          }
        }
      }
    }

    trait ByKey[F[_]] {
      import ByKey.*

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

//        def apply(deleteTo: DeleteTo, seqNr: SeqNr): F[Unit]
//
        def apply(seqNr: SeqNr, deleteTo: DeleteTo): F[Unit]
      }
    }
  }

  final case class Statements[F[_]](
    insertRecords: JournalStatements.InsertRecords[F],
    deleteRecordsTo: JournalStatements.DeleteTo[F],
    deleteRecords: JournalStatements.Delete[F],
    metaJournal: MetaJournalStatements[F],
    selectOffset2: Pointer2Statements.SelectOffset[F],
    selectPointer2: Pointer2Statements.Select[F],
    insertPointer: PointerStatements.Insert[F],
    insertPointer2: Pointer2Statements.Insert[F],
    updatePointer: PointerStatements.Update[F],
    updatePointer2: Pointer2Statements.Update[F],
    selectTopics2: Pointer2Statements.SelectTopics[F],
  )

  object Statements {

    def apply[F[_]](implicit F: Statements[F]): Statements[F] = F

    def of[F[_]: Monad: CassandraSession: ToTry: JsonCodec.Encode](
      schema: Schema,
      consistencyConfig: CassandraConsistencyConfig,
    ): F[Statements[F]] = {
      for {
        insertRecords   <- JournalStatements.InsertRecords.of[F](schema.journal, consistencyConfig.write)
        deleteRecordsTo <- JournalStatements.DeleteTo.of[F](schema.journal, consistencyConfig.write)
        deleteRecords   <- JournalStatements.Delete.of[F](schema.journal, consistencyConfig.write)
        metaJournal     <- MetaJournalStatements.of[F](schema, consistencyConfig)
        selectOffset2   <- Pointer2Statements.SelectOffset.of[F](schema.pointer2, consistencyConfig.read)
        selectPointer2  <- Pointer2Statements.Select.of[F](schema.pointer2, consistencyConfig.read)
        insertPointer   <- PointerStatements.Insert.of[F](schema.pointer, consistencyConfig.write)
        insertPointer2  <- Pointer2Statements.Insert.of[F](schema.pointer2, consistencyConfig.write)
        updatePointer   <- PointerStatements.Update.of[F](schema.pointer, consistencyConfig.write)
        updatePointer2  <- Pointer2Statements.Update.of[F](schema.pointer2, consistencyConfig.write)
        selectTopics2   <- Pointer2Statements.SelectTopics.of[F](schema.pointer2, consistencyConfig.read)
      } yield {
        Statements(
          insertRecords,
          deleteRecordsTo,
          deleteRecords,
          metaJournal,
          selectOffset2,
          selectPointer2,
          insertPointer,
          insertPointer2,
          updatePointer,
          updatePointer2,
          selectTopics2,
        )
      }
    }
  }
}
