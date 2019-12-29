package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem}
import cats.effect.concurrent.Ref
import cats.effect.implicits._
import cats.effect.{Concurrent, Resource, Sync, Timer}
import cats.implicits._
import cats.{Applicative, Monad, Parallel}
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.catshelper.{FromFuture, LogOf, ToFuture}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.smetrics.MeasureDuration

import scala.annotation.tailrec


object ReplicatedCassandra {

  def of[F[_] : Concurrent : FromFuture : ToFuture : Parallel : Timer : CassandraCluster : CassandraSession : LogOf : MeasureDuration](
    config: EventualCassandraConfig,
    origin: Option[Origin],
    metrics: Option[ReplicatedJournal.Metrics[F]],
  ): F[ReplicatedJournal[F]] = {

    for {
      schema        <- SetupSchema[F](config.schema, origin)
      statements    <- Statements.of[F](schema)
      log           <- LogOf[F].apply(ReplicatedCassandra.getClass)
      expiryService <- ExpiryService.of[F]
    } yield {
      val segmentOf = SegmentOf[F](Segments.default)
      val journal = apply[F](config.segmentSize, segmentOf, statements, expiryService)
        .withLog(log)
      metrics
        .fold(journal) { metrics => journal.withMetrics(metrics) }
        .enhanceError
    }
  }

  def apply[F[_] : Sync : Parallel](
    segmentSize: SegmentSize,
    segmentOf: SegmentOf[F],
    statements: Statements[F],
    expiryService: ExpiryService[F],
  ): ReplicatedJournal[F] = {

    implicit val monoidUnit = Applicative.monoid[F, Unit]

    new ReplicatedJournal[F] {

      def topics = {
        statements
          .selectTopics()
          .map { _.sorted }
      }

      def journal(topic: Topic) = {

        val journal: ReplicatedTopicJournal[F] = new ReplicatedTopicJournal[F] {

          val pointers = {
            statements
              .selectPointers(topic)
              .map { a => TopicPointers(a) }
          }

          def journal(id: String) = {

            val key = Key(id = id, topic = topic)

            def journalHeadRef(segment: SegmentNr) = {
              for {
                journalHead <- statements.metaJournal(key, segment).journalHead
                ref         <- Ref[F].of(journalHead)
              } yield ref
            }

            for {
              segment        <- Resource.liftF(segmentOf(key))
              journalHeadRef <- Resource.liftF(journalHeadRef(segment))
            } yield {

              def metaJournal = statements.metaJournal(key, segment)

              def delete1(
                journalHead: JournalHead,
                deleteTo: DeleteTo,
                partitionOffset: PartitionOffset,
                timestamp: Instant
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

                      def segment(seqNr: SeqNr) = SegmentNr(seqNr, segmentSize)

                      (segment(from) to segment(deleteTo.value)).parFoldMap { segment =>
                        statements.deleteRecords(key, segment, deleteTo.value)
                      }
                    }

                    val deleteTo1 = (journalHead.seqNr min deleteTo.value).toDeleteTo

                    journalHead.deleteTo.fold {
                      delete(SeqNr.min, deleteTo1)
                    } { deleteTo =>
                      if (deleteTo >= deleteTo1) {
                        ().pure[F]
                      } else {
                        deleteTo.value.next[Option].foldMap { delete(_, deleteTo1) }
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
                  events: Nel[EventRecord]
                ) = {

                  def append(segmentSize: SegmentSize, offset: Option[Offset]) = {

                    @tailrec
                    def loop(
                      events: List[EventRecord],
                      s: Option[(Segment, Nel[EventRecord])],
                      result: F[Unit]
                    ): F[Unit] = {

                      def insert(segment: Segment, events: Nel[EventRecord]) = {
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
                  } yield {}
                }

                def delete(
                  partitionOffset: PartitionOffset,
                  timestamp: Instant,
                  deleteTo: DeleteTo,
                  origin: Option[Origin]
                ) = {

                  def insert = {
                    val journalHead = JournalHead(
                      partitionOffset = partitionOffset,
                      segmentSize = segmentSize,
                      seqNr = deleteTo.value,
                      deleteTo = deleteTo.some)
                    metaJournal
                      .insert(timestamp, journalHead, origin)
                      .as(journalHead.some)
                  }

                  def delete(journalHead: JournalHead) = {
                    delete1(journalHead, deleteTo, partitionOffset, timestamp)
                  }

                  for {
                    journalHead <- journalHeadRef.get
                    journalHead <- journalHead.fold { insert } { delete }
                    _           <- journalHead.traverse { journalHead => journalHeadRef.set(journalHead.some) }
                  } yield {}
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
                      val result = for {
                        journalHead <- delete1(journalHead, journalHead.seqNr.toDeleteTo, partitionOffset, timestamp)
                        _           <- journalHead.traverse { journalHead => journalHeadRef.set(journalHead.some) }
                        _           <- metaJournal.delete
                        _           <- journalHeadRef.set(none)
                      } yield {}
                      result.uncancelable
                    } else {
                      ().pure[F]
                    }
                  }
                  for {
                    journalHead <- journalHeadRef.get
                    result      <- journalHead.foldMapM { purge }
                  } yield result
                }
              }
            }
          }

          def save(pointers: Nem[Partition, Offset], timestamp: Instant) = {

            def insertOrUpdate(current: Option[Offset], partition: Partition, offset: Offset) = {
              current.fold {
                statements.insertPointer(
                  topic = topic,
                  partition = partition,
                  offset = offset,
                  created = timestamp,
                  updated = timestamp)
              } { _ =>
                statements.updatePointer(
                  topic = topic,
                  partition = partition,
                  offset = offset,
                  timestamp = timestamp)
              }
            }

            def saveOne(partition: Partition, offset: Offset) = {
              for {
                current <- statements.selectPointer(topic, partition)
                result  <- insertOrUpdate(current, partition, offset)
              } yield result
            }

            def saveMany(pointers: Nel[(Partition, Offset)]) = {
              val partitions = pointers.map { case (partition, _) => partition }
              for {
                current <- statements.selectPointersIn(topic, partitions)
                result  <- pointers.parFoldMap { case (partition, offset) => insertOrUpdate(current.get(partition), partition, offset) }
              } yield result
            }

            pointers.toNel match {
              case Nel((partition, offset), Nil) => saveOne(partition, offset)
              case pointers                      => saveMany(pointers)
            }
          }
        }

        Resource.liftF(journal.pure[F])
      }
    }
  }


  trait MetaJournalStatements[F[_]] {
    import MetaJournalStatements._

    def apply(key: Key, segment: SegmentNr): ByKey[F]
  }

  object MetaJournalStatements {

    def of[F[_] : Monad : Parallel : CassandraSession](schema: Schema): F[MetaJournalStatements[F]] = {
      val metadata = {
        val statements = (
          MetadataStatements.SelectJournalHead.of[F](schema.metadata),
          MetadataStatements.Insert.of[F](schema.metadata),
          MetadataStatements.Update.of[F](schema.metadata),
          MetadataStatements.UpdateSeqNr.of[F](schema.metadata),
          MetadataStatements.UpdateDeleteTo.of[F](schema.metadata),
          MetadataStatements.Delete.of[F](schema.metadata))
        statements.parMapN(apply[F])
      }

      schema
        .metaJournal
        .fold {
          metadata
        } { metaJournal =>
          val select = MetadataStatements.Select.of[F](schema.metadata)
          val insert = cassandra.MetaJournalStatements.Insert.of[F](metaJournal)
          (of[F](metaJournal), metadata, select, insert).parMapN(apply[F])
        }
    }


    def of[F[_] : Monad : Parallel : CassandraSession](metaJournal: TableName): F[MetaJournalStatements[F]] = {
      val statements = (
        cassandra.MetaJournalStatements.SelectJournalHead.of[F](metaJournal),
        cassandra.MetaJournalStatements.Insert.of[F](metaJournal),
        cassandra.MetaJournalStatements.Update.of[F](metaJournal),
        cassandra.MetaJournalStatements.UpdateSeqNr.of[F](metaJournal),
        cassandra.MetaJournalStatements.UpdateExpiry.of[F](metaJournal),
        cassandra.MetaJournalStatements.UpdateDeleteTo.of[F](metaJournal),
        cassandra.MetaJournalStatements.Delete.of[F](metaJournal),
        cassandra.MetaJournalStatements.DeleteExpiry.of[F](metaJournal))
      statements.parMapN(apply[F])
    }


    def apply[F[_] : Monad](
      metaJournal      : MetaJournalStatements[F],
      metadata         : MetaJournalStatements[F],
      selectMetadata   : MetadataStatements.Select[F],
      insertMetaJournal: cassandra.MetaJournalStatements.Insert[F]
    ): MetaJournalStatements[F] = {

      new MetaJournalStatements[F] {

        def apply(key: Key, segment: SegmentNr) = {

          def metaJournal1 = metaJournal(key, segment)

          def metadata1 = metadata(key, segment)

          new ByKey[F] {

            def journalHead = {
              metaJournal1
                .journalHead
                .flatMap { journalHead =>
                  journalHead.fold {
                    selectMetadata(key).flatMap { entry =>
                      entry.traverse { entry =>
                        val journalHead = entry.journalHead
                        insertMetaJournal(
                          key = key,
                          segment = segment,
                          created = entry.created,
                          updated = entry.updated,
                          journalHead = journalHead,
                          origin = entry.origin
                        ) as journalHead
                      }
                    }
                  } { journalHead =>
                    journalHead.some.pure[F]
                  }
                }
            }

            def insert(timestamp: Instant, journalHead: JournalHead, origin: Option[Origin]) = {
              for {
                _ <- metadata1.insert(timestamp, journalHead, origin)
                _ <- metaJournal1.insert(timestamp, journalHead, origin)
              } yield {}
            }

            def update(partitionOffset: PartitionOffset, timestamp: Instant) = {

              def metaJournal = metaJournal1.update(partitionOffset, timestamp)

              def metadata = metadata1.update(partitionOffset, timestamp)

              new ByKey.Update[F] {

                def apply(seqNr: SeqNr) = {
                  for {
                    _ <- metaJournal(seqNr)
                    _ <- metadata(seqNr)
                  } yield {}
                }

                def apply(seqNr: SeqNr, expiry: Expiry) = {
                  for {
                    _ <- metaJournal(seqNr, expiry)
                    _ <- metadata(seqNr, expiry)
                  } yield {}
                }

                def apply(deleteTo: DeleteTo) = {
                  for {
                    _ <- metaJournal(deleteTo)
                    _ <- metadata(deleteTo)
                  } yield {}
                }

                def apply(seqNr: SeqNr, deleteTo: DeleteTo) = {
                  for {
                    _ <- metaJournal(seqNr, deleteTo)
                    _ <- metadata(seqNr, deleteTo)
                  } yield {}
                }
              }
            }

            def delete = {
              for {
                _ <- metaJournal1.delete
                _ <- metadata1.delete
              } yield {}
            }

            def deleteExpiry = metaJournal1.deleteExpiry
          }
        }
      }
    }


    def apply[F[_]](
      selectJournalHead: cassandra.MetaJournalStatements.SelectJournalHead[F],
      insert           : cassandra.MetaJournalStatements.Insert[F],
      update           : cassandra.MetaJournalStatements.Update[F],
      updateSeqNr      : cassandra.MetaJournalStatements.UpdateSeqNr[F],
      updateExpiry     : cassandra.MetaJournalStatements.UpdateExpiry[F],
      updateDeleteTo   : cassandra.MetaJournalStatements.UpdateDeleteTo[F],
      delete           : cassandra.MetaJournalStatements.Delete[F],
      deleteExpiry     : cassandra.MetaJournalStatements.DeleteExpiry[F]
    ): MetaJournalStatements[F] = {

      val inset1 = insert
      val update1 = update
      val delete1 = delete
      val deleteExpiry1 = deleteExpiry

      new MetaJournalStatements[F] {

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


    def apply[F[_] : Applicative](
      selectJournalHead: MetadataStatements.SelectJournalHead[F],
      insert           : MetadataStatements.Insert[F],
      update           : MetadataStatements.Update[F],
      updateSeqNr      : MetadataStatements.UpdateSeqNr[F],
      updateDeleteTo   : MetadataStatements.UpdateDeleteTo[F],
      delete           : MetadataStatements.Delete[F]
    ): MetaJournalStatements[F] = {

      val insert1 = insert
      val update1 = update
      val delete1 = delete

      new MetaJournalStatements[F] {
        
        def apply(key: Key, segment: SegmentNr) = {
          new ByKey[F] {

            def journalHead = selectJournalHead(key)

            def insert(timestamp: Instant, journalHead: JournalHead, origin: Option[Origin]) = {
              insert1(key, timestamp, journalHead, origin)
            }

            def update(partitionOffset: PartitionOffset, timestamp: Instant) = {
              new ByKey.Update[F] {

                def apply(seqNr: SeqNr) = {
                  updateSeqNr(key, partitionOffset, timestamp, seqNr)
                }

                def apply(seqNr: SeqNr, expiry: Expiry) = {
                  updateSeqNr(key, partitionOffset, timestamp, seqNr)
                }

                def apply(deleteTo: DeleteTo) = {
                  updateDeleteTo(key, partitionOffset, timestamp, deleteTo)
                }

                def apply(seqNr: SeqNr, deleteTo: DeleteTo) = {
                  update1(key, partitionOffset, timestamp, seqNr, deleteTo)
                }
              }
            }

            def delete = delete1(key)

            def deleteExpiry = ().pure[F]
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
    insertRecords   : JournalStatements.InsertRecords[F],
    deleteRecords   : JournalStatements.DeleteRecords[F],
    metaJournal     : MetaJournalStatements[F],
    selectPointer   : PointerStatements.Select[F],
    selectPointersIn: PointerStatements.SelectIn[F],
    selectPointers  : PointerStatements.SelectAll[F],
    insertPointer   : PointerStatements.Insert[F],
    updatePointer   : PointerStatements.Update[F],
    selectTopics    : PointerStatements.SelectTopics[F])

  object Statements {

    def apply[F[_]](implicit F: Statements[F]): Statements[F] = F

    def of[F[_] : Monad : Parallel : CassandraSession](schema: Schema): F[Statements[F]] = {
      val statements = (
        JournalStatements.InsertRecords.of[F](schema.journal),
        JournalStatements.DeleteRecords.of[F](schema.journal),
        MetaJournalStatements.of[F](schema),
        PointerStatements.Select.of[F](schema.pointer),
        PointerStatements.SelectIn.of[F](schema.pointer),
        PointerStatements.SelectAll.of[F](schema.pointer),
        PointerStatements.Insert.of[F](schema.pointer),
        PointerStatements.Update.of[F](schema.pointer),
        PointerStatements.SelectTopics.of[F](schema.pointer))
      statements.parMapN(Statements[F])
    }
  }
}