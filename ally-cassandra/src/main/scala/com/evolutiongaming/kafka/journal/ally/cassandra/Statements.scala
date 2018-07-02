package com.evolutiongaming.kafka.journal.ally.cassandra

import java.lang.{Long => LongJ}
import java.nio.ByteBuffer
import java.util.Date

import com.datastax.driver.core.{Metadata => _, _}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.kafka.journal.Alias.{Id, SeqNr}
import com.evolutiongaming.kafka.journal.SeqRange
import com.evolutiongaming.kafka.journal.ally.{AllyRecord, AllyRecord2, PartitionOffset}
import com.evolutiongaming.util.FutureHelper._

import scala.collection.JavaConverters._
import scala.concurrent.Future

// TODO split journal and metadata statements
object Statements {

  def createKeyspace(keyspace: SchemaConfig.Keyspace): String = {
    // TODO make sure two parallel instances does do the same
    s"""
       |CREATE KEYSPACE IF NOT EXISTS ${ keyspace.name }
       |WITH REPLICATION = { 'class' : ${ keyspace.replicationStrategy.asCql } }
       |""".stripMargin
  }

  def createJournal(name: String): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS $name (
       |id text,
       |segment bigint,
       |seq_nr bigint,
       |timestamp timestamp,
       |payload blob,
       |tags set<text>,
       |partition int,
       |offset bigint,
       |PRIMARY KEY ((id, segment), seq_nr, timestamp))
       |""".stripMargin
    //        WITH gc_grace_seconds =${gcGrace.toSeconds} TODO
    //        AND compaction = ${config.tableCompactionStrategy.asCQL}
  }

  def createMetadata(name: String): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS $name (
       |id text PRIMARY KEY,
       |topic text,
       |segment_size int,
       |deleted_to bigint,
       |properties map<text,text>,
       |created timestamp,
       |updated timestamp)
       |""".stripMargin
  }


  trait PrepareAndExecute {
    def prepare(query: String): Future[PreparedStatement]
    def execute(statement: BoundStatement): Future[ResultSet]
  }


  object InsertRecord {
    type Type = (AllyRecord, Segment) => BoundStatement

    // TODO fast future
    // TODO create Prepare -> Run function
    def apply(name: String, prepare: String => Future[PreparedStatement]): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove
      val query =
        s"""
           |INSERT INTO $name (id, segment, seq_nr, timestamp, payload, tags, partition, offset)
           |VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- prepare(query)
      } yield {
        (record: AllyRecord, segment: Segment) => {
          // TODO make up better way for creating queries
          prepared
            .bind()
            .setString("id", record.id)
            .setLong("segment", segment.value)
            .setLong("seq_nr", record.seqNr)
            .setTimestamp("timestamp", new Date(record.timestamp))
            .setBytes("payload", ByteBuffer.wrap(record.payload))
            .setSet("tags", record.tags.asJava, classOf[String])
            .setInt("partition", record.partitionOffset.partition)
            .setLong("offset", record.partitionOffset.offset)
        }
      }
    }
  }


  // TODO rename along with AllyRecord2
  object SelectLastRecord {
    // TODO add from ?
    type Type = (Id, SeqNr, Segment) => Future[Option[AllyRecord2]]

    // TODO fast future
    // TODO create Prepare -> Run function
    def apply(name: String, session: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove
      val query =
        s"""
           |SELECT seq_nr, partition, offset
           |FROM $name
           |WHERE id = ?
           |AND segment = ?
           |AND seq_nr >= ?
           |ORDER BY seq_nr
           |DESC LIMIT 1
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        (id: Id, from: SeqNr, segment: Segment) =>
          val bound = prepared.bind(id, segment.value: LongJ, from: LongJ)
          for {
            result <- session.execute(bound)
          } yield for {
            row <- Option(result.one())
          } yield {
            val partitionOffset = PartitionOffset(
              partition = row.getInt("partition"),
              offset = row.getLong("offset"))
            AllyRecord2(
              seqNr = row.getLong("seq_nr"),
              partitionOffset = partitionOffset)
          }
      }
    }
  }


  object SelectRecords {
    type Type = (Id, SeqRange, Segment) => Future[Vector[AllyRecord]]

    // TODO fast future
    // TODO create Prepare -> Run function
    def apply(name: String, session: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove
      val query =
        s"""
           |SELECT seq_nr, timestamp, payload, tags, partition, offset FROM $name
           |WHERE id = ?
           |AND segment = ?
           |AND seq_nr >= ?
           |AND seq_nr <= ?
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        (id: Id, range: SeqRange, segment: Segment) =>

          // TODO avoid casting via providing implicit converters
          val bound = prepared.bind(id, segment.value: LongJ, range.from: LongJ, range.to: LongJ)
          for {
            result <- session.execute(bound)
          } yield {
            // TODO fetch batch by batch
            val xs = result.all().asScala.toVector.map { row =>
              AllyRecord(
                id = id,
                seqNr = row.getLong("seq_nr"),
                timestamp = row.getTimestamp("timestamp").getTime,
                payload = row.getBytes("payload").array(),
                tags = row.getSet("tags", classOf[String]).asScala.toSet,
                partitionOffset = PartitionOffset(
                  partition = row.getInt("partition"),
                  offset = row.getLong("offset")))
            }

            //            if(id == "p-17") {
            //              println(s"Statements.list id: $id, segment: $segment, range: $range, result ${xs.map{_.seqNr}}")
            //            }

            xs
          }
      }
    }
  }


  object InsertMetadata {
    type Type = Metadata => Future[Unit]

    def apply(name: String, session: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove

      val query =
        s"""
           |INSERT INTO $name (id, topic, segment_size, deleted_to, properties, created, updated)
           |VALUES (?, ?, ?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        metadata: Metadata =>
          val bound = prepared
            .bind()
            .setString("id", metadata.id)
            .setString("topic", metadata.topic)
            .setInt("segment_size", metadata.segmentSize)
            .setLong("deleted_to", 0)
            .setTimestamp("created", new Date(metadata.created))
            .setTimestamp("updated", new Date(metadata.updated))
          session.execute(bound).unit
      }
    }
  }


  object SelectMetadata {
    type Type = Id => Future[Option[Metadata]]

    def apply(name: String, session: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove

      val query =
        s"""
           |SELECT topic, segment_size, created, updated FROM $name
           |WHERE id = ?
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        id: Id =>
          val bound = prepared.bind(id)
          for {
            result <- session.execute(bound)
          } yield for {
            row <- Option(result.one())
          } yield {
            Metadata(
              id = id,
              topic = row.getString("topic"),
              segmentSize = row.getInt("segment_size"),
              created = row.getTimestamp("created").getTime,
              updated = row.getTimestamp("updated").getTime)
          }
      }
    }
  }

  object SelectSegmentSize {
    type Type = Id => Future[Option[Int]]

    def apply(name: String, session: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove

      val query =
        s"""
           |SELECT segment_size FROM $name
           |WHERE id = ?
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        id: Id =>
          val bound = prepared.bind(id)
          for {
            result <- session.execute(bound)
          } yield for {
            row <- Option(result.one())
          } yield {
            row.getInt("segment_size")
          }
      }
    }
  }
}

