package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem}
import cats.effect.implicits._
import cats.effect.{Async, Ref, Sync}
import cats.syntax.all._
import cats.{Applicative, Monad, Parallel}
import com.evolutiongaming.catshelper.DataHelper._
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.catshelper.{LogOf, ToTry}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.smetrics.MeasureDuration

import java.time.Instant
import scala.annotation.tailrec


object ReplicatedCassandra {

  private sealed abstract class Main

  def of[
    F[_]
    : Async
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
    } yield {
      val segmentOf = SegmentNrsOf[F](Segments.old, Segments.default)
      val journal = apply[F](config.segmentSize, segmentOf, statements, expiryService)
        .withLog(log)
      metrics
        .fold(journal) { metrics => journal.withMetrics(metrics) }
        .enhanceError
    }
  }

  def apply[F[_]: Sync: Parallel: Fail](
    segmentSize: SegmentSize,
    segmentNrsOf: SegmentNrsOf[F],
    statements: Statements[F],
    expiryService: ExpiryService[F],
  ): ReplicatedJournal[F] = {

    implicit val monoidUnit = Applicative.monoid[F, Unit]

    new Main with ReplicatedJournal[F] {

      def topics = {
        statements
          .selectTopics()
          .map { _.toSortedSet }
      }

      def journal(topic: Topic) = {

        val journal = for {
          pointersRef <- Ref[F].of(TopicPointers.empty)
        } yield {
          new ReplicatedTopicJournal[F] {

            val pointers = {
              statements
                .selectPointers(topic)
                .map { a => TopicPointers(a) }
                .flatTap { pointers => pointersRef.update { _.merge(pointers) } }
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
                  ref        <- Ref[F].of(head)
                } yield {
                  (ref, segmentNr)
                }
              }

              val result = for {
                value <- journalHeadRef
              } yield {
                val (journalHeadRef, segmentNr) = value

                def metaJournal = statements.metaJournal(key, segmentNr)

                def delete1(
                  journalHead: JournalHead,
                  deleteTo: DeleteTo,
                  partitionOffset: PartitionOffset,
                  timestamp: Instant,
                  purge: Boolean
                ) = {

                  if (partitionOffset.offset <= journalHead.partitionOffset.offset) {
                    none[JournalHead].pure[F]
                  } else {
                    def update = {
                      def update = metaJournal.update(partitionOffset, timestamp)

                      if (journalHead.seqNr >= deleteTo.value) {
                        val journalHead1 = journalHead.copy(
                          partitionOffset = partitionOffset,
                          deleteTo = deleteTo.some)
                        update(deleteTo).as(journalHead1)
                      } else {
                        val journalHead1 = journalHead.copy(
                          partitionOffset = partitionOffset,
                          seqNr = deleteTo.value,
                          deleteTo = deleteTo.some)
                        update(deleteTo.value, deleteTo).as(journalHead1)
                      }
                    }

                    def delete = {

                      def delete(from: SeqNr, deleteTo: DeleteTo) = {

                        def segmentNr(seqNr: SeqNr) = SegmentNr(seqNr, journalHead.segmentSize)

                        segmentNr(from)
                          .to[F](segmentNr(deleteTo.value))
                          .flatMap { segmentNrs =>
                            val deletes = if (journalHead.seqNr <= deleteTo.value) {
                              segmentNrs.foldLeft(List.empty[F[Unit]]) { (result, a) =>
                                statements.deleteRecords(key, a) :: result
                              }
                            } else {
                              @tailrec def loop(as: List[SegmentNr], result: List[F[Unit]]): List[F[Unit]] = {
                                as match {
                                  case Nil      => result
                                  case a :: Nil => statements.deleteRecordsTo(key, a, deleteTo.value) :: result
                                  case a :: as  => loop(as, statements.deleteRecords(key, a) :: result)
                                }
                              }

                              loop(segmentNrs, List.empty)
                            }

                            deletes.parFoldMapA { _.uncancelable }
                          }
                      }

                      val deleteTo1 = (journalHead.seqNr min deleteTo.value).toDeleteTo

                      journalHead.deleteTo.fold {
                        delete(SeqNr.min, deleteTo1)
                      } { deleteTo =>
                        if (purge) {
                          delete(SeqNr.min, deleteTo1)
                        } else if (deleteTo >= deleteTo1) {
                          ().pure[F]
                        } else {
                          deleteTo
                            .value
                            .next[Option]
                            .foldMap { from => delete(from, deleteTo1) }
                        }
                      }
                    }

                    val result = for {
                      journalHead <- update
                      _           <- delete
                    } yield {
                      journalHead.some
                    }
                    result.uncancelable
                  }
                }

                new ReplicatedKeyJournal[F] {

                  def append(
                    partitionOffset: PartitionOffset,
                    timestamp: Instant,
                    expireAfter: Option[ExpireAfter],
                    events: Nel[EventRecord[EventualPayloadAndType]]
                  ) = {

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
                                segmentSize = segmentSize,
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
                          saveAndJournalHead  <- saveAndJournalHead
                          (save, journalHead)  = saveAndJournalHead
                          _                   <- append(journalHead.segmentSize, offset)
                          _                   <- save
                        } yield {
                          journalHead.some
                        }
                        result.uncancelable
                      }

                      journalHead.fold {
                        appendAndSave
                      } { journalHead =>
                        if (partitionOffset.offset <= journalHead.partitionOffset.offset) {
                          none[JournalHead].pure[F]
                        } else {
                          appendAndSave
                        }
                      }
                    }

                    for {
                      journalHead <- journalHeadRef.get
                      journalHead <- appendAndSave(journalHead)
                      _           <- journalHead.traverse { journalHead => journalHeadRef.set(journalHead.some) }
                    } yield journalHead.isDefined
                  }

                  def delete(
                    partitionOffset: PartitionOffset,
                    timestamp: Instant,
                    deleteTo: DeleteTo,
                    origin: Option[Origin]
                  ) = {
                    for {
                      journalHead <- journalHeadRef.get
                      journalHead <- journalHead match {
                        case Some(journalHead) =>
                          delete1(journalHead, deleteTo, partitionOffset, timestamp, purge = false)
                        case None              =>
                          val journalHead = JournalHead(
                            partitionOffset = partitionOffset,
                            segmentSize = segmentSize,
                            seqNr = deleteTo.value,
                            deleteTo = deleteTo.some)
                          metaJournal
                            .insert(timestamp, journalHead, origin)
                            .as(journalHead.some)
                      }
                      _           <- journalHead.traverse { journalHead => journalHeadRef.set(journalHead.some) }
                    } yield {
                      journalHead.isDefined
                    }
                  }

                  def purge(
                    offset: Offset,
                    timestamp: Instant
                  ) = {

                    def purge(journalHead: JournalHead) = {
                      if (offset > journalHead.partitionOffset.offset) {
                        val partitionOffset = journalHead
                          .partitionOffset
                          .copy(offset = offset)
                        val deleteTo = journalHead.seqNr.toDeleteTo
                        val result = for {
                          journalHead <- delete1(journalHead, deleteTo, partitionOffset, timestamp, purge = true)
                          _           <- journalHead.foldMapM { journalHead => journalHeadRef.set(journalHead.some) }
                          _           <- metaJournal.delete
                          _           <- journalHeadRef.set(none)
                        } yield {
                          journalHead.isDefined
                        }
                        result.uncancelable
                      } else {
                        false.pure[F]
                      }
                    }

                    for {
                      journalHead <- journalHeadRef.get
                      result      <- journalHead.fold(false.pure[F]) { purge }
                    } yield result
                  }
                }
              }

              result.toResource
            }


            def save(pointers: Nem[Partition, Offset], timestamp: Instant) = {

              def insert(partition: Partition, offset: Offset) = {
                statements.insertPointer(
                  topic = topic,
                  partition = partition,
                  offset = offset,
                  created = timestamp,
                  updated = timestamp)
              }

              def update(partition: Partition, offset: Offset) = {
                statements.updatePointer(
                  topic = topic,
                  partition = partition,
                  offset = offset,
                  timestamp = timestamp)
              }

              for {
                pointers  <- pointers.toNel.pure[F]
                pointers0 <- pointersRef.get
                pointers0 <- {
                  pointers
                    .collect { case (partition, _) if !pointers0.values.contains(partition) => partition }
                    .toNel
                    .fold {
                      pointers0.pure[F]
                    } { partitions =>
                      for {
                        missing  <- partitions match {
                          case Nel(partition, Nil) =>
                            statements
                              .selectPointer(topic, partition)
                              .map { offset =>
                                val pointers = offset
                                  .map { offset => (partition, offset) }
                                  .toMap
                                TopicPointers(pointers)
                              }

                          case partitions =>
                            statements
                              .selectPointersIn(topic, partitions)
                              .map { pointers => TopicPointers(pointers) }
                        }
                        pointers <- {
                          if (missing.values.nonEmpty) {
                            pointersRef.modify { pointers =>
                              val pointers1 = pointers.merge(missing)
                              (pointers1, pointers1)
                            }
                          } else {
                            pointers0.pure[F]
                          }
                        }
                      } yield pointers
                    }
                }

                changed  <- pointers.parFoldMap { case (partition, offset) =>
                  pointers0
                    .values
                    .get(partition)
                    .fold {
                      insert(partition, offset).as(1)
                    } { current =>
                      if (current < offset) {
                        update(partition, offset).as(1)
                      } else {
                        0.pure[F]
                      }
                    }
                }

                pointers <- TopicPointers(pointers.toList.toMap).pure[F]
                changed  <- (changed > 0).pure[F]
                _        <- if (changed) pointersRef.update { _.merge(pointers) } else ().pure[F]
              } yield {
                changed
              }
            }
          }
        }
        journal.toResource
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

      for {
        selectMetadata <- MetadataStatements.Select.of[F](schema.metadata, consistencyConfig.read)
        deleteMetadata <- MetadataStatements.Delete.of[F](schema.metadata, consistencyConfig.write)
        insertMetadata <- cassandra.MetaJournalStatements.Insert.of[F](schema.metaJournal, consistencyConfig.write)
        metaJournal    <- of[F](schema.metaJournal, consistencyConfig)
      } yield {
        apply(metaJournal, selectMetadata, deleteMetadata, insertMetadata)
      }
    }


    def of[F[_]: Monad: CassandraSession](
      metaJournal: TableName,
      consistencyConfig: ConsistencyConfig
    ): F[MetaJournalStatements[F]] = {

      for {
        selectJournalHead <- cassandra.MetaJournalStatements.SelectJournalHead.of[F](metaJournal, consistencyConfig.read)
        insert            <- cassandra.MetaJournalStatements.Insert.of[F](metaJournal, consistencyConfig.write)
        update            <- cassandra.MetaJournalStatements.Update.of[F](metaJournal, consistencyConfig.write)
        updateSeqNr       <- cassandra.MetaJournalStatements.UpdateSeqNr.of[F](metaJournal, consistencyConfig.write)
        updateExpiry      <- cassandra.MetaJournalStatements.UpdateExpiry.of[F](metaJournal, consistencyConfig.write)
        updateDeleteTo    <- cassandra.MetaJournalStatements.UpdateDeleteTo.of[F](metaJournal, consistencyConfig.write)
        delete            <- cassandra.MetaJournalStatements.Delete.of[F](metaJournal, consistencyConfig.write)
        deleteExpiry      <- cassandra.MetaJournalStatements.DeleteExpiry.of[F](metaJournal, consistencyConfig.write)
      } yield {
        apply(selectJournalHead, insert, update, updateSeqNr, updateExpiry, updateDeleteTo, delete, deleteExpiry)
      }
    }


    private sealed abstract class Main

    def apply[F[_]: Monad](
      metaJournal: MetaJournalStatements[F],
      selectMetadata: MetadataStatements.Select[F],
      deleteMetadata: MetadataStatements.Delete[F],
      insertMetaJournal: cassandra.MetaJournalStatements.Insert[F]
    ): MetaJournalStatements[F] = {

      new Main with MetaJournalStatements[F] {

        def apply(key: Key, segment: SegmentNr) = {

          def metaJournal1 = metaJournal(key, segment)

          new ByKey[F] {

            def journalHead = {
              metaJournal1
                .journalHead
                .flatMap {
                  case Some(journalHead) =>
                    journalHead.some.pure[F]
                  case None =>
                    selectMetadata(key).flatMap { entry =>
                      entry.traverse { entry =>
                        val journalHead = entry.journalHead
                        def insert = insertMetaJournal(
                          key = key,
                          segment = segment,
                          created = entry.created,
                          updated = entry.updated,
                          journalHead = journalHead,
                          origin = entry.origin
                        )
                        for {
                          _ <- insert
                          _ <- deleteMetadata(key)
                        } yield journalHead
                      }
                    }
                }
            }

            def insert(timestamp: Instant, journalHead: JournalHead, origin: Option[Origin]) = {
              metaJournal1.insert(timestamp, journalHead, origin)
            }

            def update(partitionOffset: PartitionOffset, timestamp: Instant) = {

              def metaJournal = metaJournal1.update(partitionOffset, timestamp)

              new ByKey.Update[F] {

                def apply(seqNr: SeqNr) = metaJournal(seqNr)

                def apply(seqNr: SeqNr, expiry: Expiry) = metaJournal(seqNr, expiry)

                def apply(deleteTo: DeleteTo) = metaJournal(deleteTo)

                def apply(seqNr: SeqNr, deleteTo: DeleteTo) = metaJournal(seqNr, deleteTo)
              }
            }

            def delete = {
              for {
                _ <- deleteMetadata(key)
                a <- metaJournal1.delete
              } yield a
            }

            def deleteExpiry = metaJournal1.deleteExpiry
          }
        }
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
      delete: cassandra.MetaJournalStatements.Delete[F],
      deleteExpiry: cassandra.MetaJournalStatements.DeleteExpiry[F]
    ): MetaJournalStatements[F] = {

      val inset1 = insert
      val update1 = update
      val delete1 = delete
      val deleteExpiry1 = deleteExpiry

      new MetaJournal with MetaJournalStatements[F] {

        def apply(key: Key, segment: SegmentNr) = {
          new ByKey[F] {

            def journalHead = selectJournalHead(key, segment)

            def insert(timestamp: Instant, journalHead: JournalHead, origin: Option[Origin]) = {
              inset1(key, segment, timestamp, timestamp, journalHead, origin)
            }

            def update(partitionOffset: PartitionOffset, timestamp: Instant) = {
              new ByKey.Update[F] {

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
    selectPointer: PointerStatements.Select[F],
    selectPointersIn: PointerStatements.SelectIn[F],
    selectPointers: PointerStatements.SelectAll[F],
    insertPointer: PointerStatements.Insert[F],
    updatePointer: PointerStatements.Update[F],
    selectTopics: PointerStatements.SelectTopics[F])

  object Statements {

    def apply[F[_]](implicit F: Statements[F]): Statements[F] = F

    def of[F[_]: Monad: Parallel: CassandraSession: ToTry: JsonCodec.Encode](
      schema: Schema,
      consistencyConfig: ConsistencyConfig
    ): F[Statements[F]] = {

      val statements = (
        JournalStatements.InsertRecords.of[F](schema.journal, consistencyConfig.write),
        JournalStatements.DeleteTo.of[F](schema.journal, consistencyConfig.write),
        JournalStatements.Delete.of[F](schema.journal, consistencyConfig.write),
        MetaJournalStatements.of[F](schema, consistencyConfig),
        PointerStatements.Select.of[F](schema.pointer, consistencyConfig.read),
        PointerStatements.SelectIn.of[F](schema.pointer, consistencyConfig.read),
        PointerStatements.SelectAll.of[F](schema.pointer, consistencyConfig.read),
        PointerStatements.Insert.of[F](schema.pointer, consistencyConfig.write),
        PointerStatements.Update.of[F](schema.pointer, consistencyConfig.write),
        PointerStatements.SelectTopics.of[F](schema.pointer, consistencyConfig.read))
      statements.parMapN(Statements[F])
    }
  }
}
