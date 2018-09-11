package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import com.evolutiongaming.kafka.journal.FoldWhileHelper.Switch
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.{ReplicatedJournal, TopicPointers}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords, OffsetAndMetadata, WithSize}
import com.evolutiongaming.skafka.{Bytes => _, _}
import org.scalatest.{Matchers, WordSpec}

import scala.util.control.NonFatal

class TopicReplicatorSpec extends WordSpec with Matchers {
  import TopicReplicatorSpec._

  "TopicReplicator" should {

    "replicate appends" in {
      val records = {
        val records = for {
          partition <- 0 to 1
        } yield {

          def keyOf(id: String) = Key(id = s"$partition-$id", topic = topic)

          def append(id: String, seqNrs: Nel[Int]) = {
            val key = keyOf(id)
            kafkaRecord(key, seqNrs)
          }

          val topicPartition = topicPartitionOf(partition)

          val kafkaRecords = List(
            append("0", Nel(1)),
            append("1", Nel(1, 2)),
            append("0", Nel(2)),
            append("1", Nel(3)))

          val records = for {
            (record, idx) <- kafkaRecords.zipWithIndex
          } yield {
            val offset = idx + 1l
            consumerRecordOf(record, topicPartition, offset)
          }
          (topicPartition, records)
        }
        ConsumerRecords(records.toMap)
      }

      val data = Data(records = List(records))
      val io = topicReplicator.shutdown()
      val (result, _) = io.run(data)

      result shouldEqual Data(
        topics = List(topic),
        commits = List(Map(
          (topicPartitionOf(0), offsetAndMetadata(5)),
          (topicPartitionOf(1), offsetAndMetadata(5)))),
        stopped = true,
        pointers = Map((topic, TopicPointers(Map((0, 4l), (1, 4l))))),
        journal = Map(
          (Key(id = "0-0", topic = topic), List(
            replicated(seqNr = 1, partition = 0, offset = 1),
            replicated(seqNr = 2, partition = 0, offset = 3))),
          (Key(id = "0-1", topic = topic), List(
            replicated(seqNr = 1, partition = 0, offset = 2),
            replicated(seqNr = 2, partition = 0, offset = 2),
            replicated(seqNr = 3, partition = 0, offset = 4))),
          (Key(id = "1-0", topic = topic), List(
            replicated(seqNr = 1, partition = 1, offset = 1),
            replicated(seqNr = 2, partition = 1, offset = 3))),
          (Key(id = "1-1", topic = topic), List(
            replicated(seqNr = 1, partition = 1, offset = 2),
            replicated(seqNr = 2, partition = 1, offset = 2),
            replicated(seqNr = 3, partition = 1, offset = 4)))),
        metadata = Map(
          (Key(id = "0-0", topic = topic), None),
          (Key(id = "0-1", topic = topic), None),
          (Key(id = "1-0", topic = topic), None),
          (Key(id = "1-1", topic = topic), None)))
    }

    "replicate appends of many polls" in {
      val records = for {
        partition <- (0 to 1).toList
        record <- {
          def keyOf(id: String) = Key(id = s"$partition-$id", topic = topic)

          def append(id: String, seqNrs: Nel[Int]) = {
            val key = keyOf(id)
            kafkaRecord(key, seqNrs)
          }

          val topicPartition = topicPartitionOf(partition)

          val kafkaRecords = List(
            append("0", Nel(1)),
            append("1", Nel(1, 2)),
            append("0", Nel(2)),
            append("1", Nel(3)))

          for {
            (record, idx) <- kafkaRecords.zipWithIndex
          } yield {
            val offset = idx + 1l
            val consumerRecord = consumerRecordOf(record, topicPartition, offset)
            ConsumerRecords(Map((topicPartition, List(consumerRecord))))
          }
        }
      } yield record

      val data = Data(records = records)
      val io = topicReplicator.shutdown()
      val (result, _) = io.run(data)

      result shouldEqual Data(
        topics = List(topic),
        commits = List(
          Map((topicPartitionOf(1), offsetAndMetadata(5))),
          Map((topicPartitionOf(1), offsetAndMetadata(4))),
          Map((topicPartitionOf(1), offsetAndMetadata(3))),
          Map((topicPartitionOf(1), offsetAndMetadata(2))),
          Map((topicPartitionOf(0), offsetAndMetadata(5))),
          Map((topicPartitionOf(0), offsetAndMetadata(4))),
          Map((topicPartitionOf(0), offsetAndMetadata(3))),
          Map((topicPartitionOf(0), offsetAndMetadata(2)))),
        stopped = true,
        pointers = Map((topic, TopicPointers(Map((0, 4l), (1, 4l))))),
        journal = Map(
          (Key(id = "0-0", topic = topic), List(
            replicated(seqNr = 2, partition = 0, offset = 3),
            replicated(seqNr = 1, partition = 0, offset = 1))),
          (Key(id = "0-1", topic = topic), List(
            replicated(seqNr = 3, partition = 0, offset = 4),
            replicated(seqNr = 1, partition = 0, offset = 2),
            replicated(seqNr = 2, partition = 0, offset = 2))),
          (Key(id = "1-0", topic = topic), List(
            replicated(seqNr = 2, partition = 1, offset = 3),
            replicated(seqNr = 1, partition = 1, offset = 1))),
          (Key(id = "1-1", topic = topic), List(
            replicated(seqNr = 3, partition = 1, offset = 4),
            replicated(seqNr = 1, partition = 1, offset = 2),
            replicated(seqNr = 2, partition = 1, offset = 2)))),
        metadata = Map(
          (Key(id = "0-0", topic = topic), None),
          (Key(id = "0-1", topic = topic), None),
          (Key(id = "1-0", topic = topic), None),
          (Key(id = "1-1", topic = topic), None)))
    }

    "replicate appends and ignore marks" in {
      val records = {
        val records = for {
          partition <- 0 to 2
        } yield {

          def keyOf(id: String) = Key(id = s"$partition-$id", topic = topic)

          def append(id: String, seqNrs: Nel[Int]) = {
            val key = keyOf(id)
            kafkaRecord(key, seqNrs)
          }

          def mark(id: String) = {
            val key = keyOf(id)
            val mark = Action.Mark("id", timestamp)
            KafkaRecord(key, mark)
          }

          val topicPartition = topicPartitionOf(partition)

          val kafkaRecords = List(
            append("0", Nel(1)),
            mark("3"),
            append("1", Nel(1, 2)),
            mark("2"),
            append("0", Nel(2)),
            mark("1"),
            append("2", Nel(1, 2, 3)),
            append("1", Nel(3)),
            mark("0"))

          val records = for {
            (record, idx) <- kafkaRecords.zipWithIndex
          } yield {
            val offset = idx + 1l
            consumerRecordOf(record, topicPartition, offset)
          }
          (topicPartition, records)
        }
        ConsumerRecords(records.toMap)
      }

      val data = Data(records = List(records))
      val io = topicReplicator.shutdown()
      val (result, _) = io.run(data)

      result shouldEqual Data(
        topics = List(topic),
        commits = List(Map(
          (topicPartitionOf(0), offsetAndMetadata(10)),
          (topicPartitionOf(1), offsetAndMetadata(10)),
          (topicPartitionOf(2), offsetAndMetadata(10)))),
        stopped = true,
        pointers = Map((topic, TopicPointers(Map((0, 9l), (1, 9l), (2, 9l))))),
        journal = Map(
          (Key(id = "0-0", topic = topic), List(
            replicated(seqNr = 1, partition = 0, offset = 1),
            replicated(seqNr = 2, partition = 0, offset = 5))),
          (Key(id = "0-1", topic = topic), List(
            replicated(seqNr = 1, partition = 0, offset = 3),
            replicated(seqNr = 2, partition = 0, offset = 3),
            replicated(seqNr = 3, partition = 0, offset = 8))),
          (Key(id = "0-2", topic = topic), List(
            replicated(seqNr = 1, partition = 0, offset = 7),
            replicated(seqNr = 2, partition = 0, offset = 7),
            replicated(seqNr = 3, partition = 0, offset = 7))),
          (Key(id = "1-0", topic = topic), List(
            replicated(seqNr = 1, partition = 1, offset = 1),
            replicated(seqNr = 2, partition = 1, offset = 5))),
          (Key(id = "1-1", topic = topic), List(
            replicated(seqNr = 1, partition = 1, offset = 3),
            replicated(seqNr = 2, partition = 1, offset = 3),
            replicated(seqNr = 3, partition = 1, offset = 8))),
          (Key(id = "1-2", topic = topic), List(
            replicated(seqNr = 1, partition = 1, offset = 7),
            replicated(seqNr = 2, partition = 1, offset = 7),
            replicated(seqNr = 3, partition = 1, offset = 7))),
          (Key(id = "2-0", topic = topic), List(
            replicated(seqNr = 1, partition = 2, offset = 1),
            replicated(seqNr = 2, partition = 2, offset = 5))),
          (Key(id = "2-1", topic = topic), List(
            replicated(seqNr = 1, partition = 2, offset = 3),
            replicated(seqNr = 2, partition = 2, offset = 3),
            replicated(seqNr = 3, partition = 2, offset = 8))),
          (Key(id = "2-2", topic = topic), List(
            replicated(seqNr = 1, partition = 2, offset = 7),
            replicated(seqNr = 2, partition = 2, offset = 7),
            replicated(seqNr = 3, partition = 2, offset = 7)))),
        metadata = Map(
          (Key(id = "0-0", topic = topic), None),
          (Key(id = "0-1", topic = topic), None),
          (Key(id = "0-2", topic = topic), None),
          (Key(id = "1-0", topic = topic), None),
          (Key(id = "1-1", topic = topic), None),
          (Key(id = "1-2", topic = topic), None),
          (Key(id = "2-0", topic = topic), None),
          (Key(id = "2-1", topic = topic), None),
          (Key(id = "2-2", topic = topic), None)))
    }

    "replicate appends and deletes" in {
      val records = {
        val records = for {
          partition <- 0 to 1
        } yield {

          def keyOf(id: String) = Key(id = s"$partition-$id", topic = topic)

          def append(id: String, seqNrs: Nel[Int]) = {
            val key = keyOf(id)
            kafkaRecord(key, seqNrs)
          }

          def mark(id: String) = {
            val key = keyOf(id)
            val mark = Action.Mark("id", timestamp)
            KafkaRecord(key, mark)
          }

          def delete(id: String, to: Int) = {
            val key = keyOf(id)
            val mark = Action.Delete(SeqNr(to.toLong), timestamp)
            KafkaRecord(key, mark)
          }

          val topicPartition = topicPartitionOf(partition)

          val kafkaRecords = List(
            append("0", Nel(1)),
            mark("3"),
            append("1", Nel(1, 2)),
            mark("2"),
            append("0", Nel(2)),
            mark("1"),
            append("2", Nel(1, 2, 3)),
            append("1", Nel(3)),
            delete("1", 2),
            mark("0"))

          val records = for {
            (record, idx) <- kafkaRecords.zipWithIndex
          } yield {
            val offset = idx + 1l
            consumerRecordOf(record, topicPartition, offset)
          }
          (topicPartition, records)
        }
        ConsumerRecords(records.toMap)
      }

      val data = Data(records = List(records))
      val io = topicReplicator.shutdown()
      val (result, _) = io.run(data)

      result shouldEqual Data(
        topics = List(topic),
        commits = List(Map(
          (topicPartitionOf(0), offsetAndMetadata(11)),
          (topicPartitionOf(1), offsetAndMetadata(11)))),
        stopped = true,
        pointers = Map((topic, TopicPointers(Map((0, 10l), (1, 10l))))),
        journal = Map(
          (Key(id = "0-0", topic = topic), List(
            replicated(seqNr = 1, partition = 0, offset = 1),
            replicated(seqNr = 2, partition = 0, offset = 5))),
          (Key(id = "0-1", topic = topic), List(
            replicated(seqNr = 3, partition = 0, offset = 8))),
          (Key(id = "0-2", topic = topic), List(
            replicated(seqNr = 1, partition = 0, offset = 7),
            replicated(seqNr = 2, partition = 0, offset = 7),
            replicated(seqNr = 3, partition = 0, offset = 7))),
          (Key(id = "1-0", topic = topic), List(
            replicated(seqNr = 1, partition = 1, offset = 1),
            replicated(seqNr = 2, partition = 1, offset = 5))),
          (Key(id = "1-1", topic = topic), List(
            replicated(seqNr = 3, partition = 1, offset = 8))),
          (Key(id = "1-2", topic = topic), List(
            replicated(seqNr = 1, partition = 1, offset = 7),
            replicated(seqNr = 2, partition = 1, offset = 7),
            replicated(seqNr = 3, partition = 1, offset = 7)))),
        metadata = Map(
          (Key(id = "0-0", topic = topic), None),
          (Key(id = "0-1", topic = topic), Some(2)),
          (Key(id = "0-2", topic = topic), None),
          (Key(id = "1-0", topic = topic), None),
          (Key(id = "1-1", topic = topic), Some(2)),
          (Key(id = "1-2", topic = topic), None)))
    }

    "replicate appends and unbound deletes" in {
      val records = {
        val records = for {
          partition <- 0 to 0
        } yield {

          def keyOf(id: String) = Key(id = s"$partition-$id", topic = topic)

          def append(id: String, seqNrs: Nel[Int]) = {
            val key = keyOf(id)
            kafkaRecord(key, seqNrs)
          }

          def mark(id: String) = {
            val key = keyOf(id)
            val mark = Action.Mark("id", timestamp)
            KafkaRecord(key, mark)
          }

          def delete(id: String, to: Int) = {
            val key = keyOf(id)
            val mark = Action.Delete(SeqNr(to.toLong), timestamp)
            KafkaRecord(key, mark)
          }

          val topicPartition = topicPartitionOf(partition)

          val kafkaRecords = List(
            append("0", Nel(1)),
            mark("3"),
            append("1", Nel(1, 2)),
            mark("2"),
            append("0", Nel(2)),
            mark("1"),
            append("2", Nel(1, 2, 3)),
            append("1", Nel(3)),
            delete("1", 2),
            delete("0", 5),
            delete("0", 6),
            delete("1", 2))

          val records = for {
            (record, idx) <- kafkaRecords.zipWithIndex
          } yield {
            val offset = idx + 1l
            consumerRecordOf(record, topicPartition, offset)
          }
          (topicPartition, records)
        }
        ConsumerRecords(records.toMap)
      }

      val data = Data(records = List(records))
      val io = topicReplicator.shutdown()
      val (result, _) = io.run(data)

      result shouldEqual Data(
        topics = List(topic),
        commits = List(Map(
          (topicPartitionOf(0), offsetAndMetadata(13)))),
        stopped = true,
        pointers = Map((topic, TopicPointers(Map((0, 12l))))),
        journal = Map(
          (Key(id = "0-0", topic = topic), Nil),
          (Key(id = "0-1", topic = topic), List(
            replicated(seqNr = 3, partition = 0, offset = 8))),
          (Key(id = "0-2", topic = topic), List(
            replicated(seqNr = 1, partition = 0, offset = 7),
            replicated(seqNr = 2, partition = 0, offset = 7),
            replicated(seqNr = 3, partition = 0, offset = 7)))),
        metadata = Map(
          (Key(id = "0-0", topic = topic), Some(2)),
          (Key(id = "0-1", topic = topic), Some(2)),
          (Key(id = "0-2", topic = topic), None)))
    }

    "consume since replicated offset" in {
      val pointers = Map((topic, TopicPointers(Map((0, 1l), (2, 2l)))))
      val data = Data(pointers = pointers)
      val io = topicReplicator.shutdown()
      val (result, _) = io.run(data)
      result shouldEqual Data(
        topics = List(topic),
        stopped = true,
        pointers = pointers)
    }
  }

  private def consumerRecordOf(
    record: KafkaRecord.Any,
    topicPartition: TopicPartition,
    offset: Offset) = {

    val producerRecord = record.toProducerRecord
    ConsumerRecord(
      topicPartition = topicPartition,
      offset = offset,
      timestampAndType = Some(timestampAndType),
      key = producerRecord.key.map(WithSize(_, 0/*TODO remove*/)),
      value = producerRecord.value.map(WithSize(_, 0/*TODO remove*/)),
      headers = producerRecord.headers)
  }

  private def replicated(seqNr: Int, partition: Partition, offset: Offset) = {
    val partitionOffset = PartitionOffset(partition = partition, offset = offset)
    val event = Event(SeqNr(seqNr.toLong), Set(seqNr.toString))
    ReplicatedEvent(event, timestamp, partitionOffset)
  }

  private def kafkaRecord(key: Key, seqNrs: Nel[Int]) = {
    val events = seqNrs.map { seqNr => Event(SeqNr(seqNr.toLong), Set(seqNr.toString)) }
    val bytes = EventsSerializer.toBytes(events)
    val range = SeqRange(events.head.seqNr, events.last.seqNr)
    val append = Action.Append(range, timestamp = timestamp, bytes)
    KafkaRecord(key, append)
  }
}

object TopicReplicatorSpec {

  val topic = "topic"
  val partitions = Set(0, 1, 2, 3, 4)

  val timestamp = Instant.now()

  val timestampAndType = TimestampAndType(timestamp, TimestampType.Create)

  def topicPartitionOf(partition: Partition) = TopicPartition(topic, partition)

  def offsetAndMetadata(offset: Offset) = OffsetAndMetadata(offset)

  val consumer: KafkaConsumer[TestIO] = new KafkaConsumer[TestIO] {

    def subscribe(topic: Topic) = TestIO { _.subscribe(topic) }

    //    def seek(topic: Topic, partitionOffsets: List[PartitionOffset]) = {
    //      TestIO { _.seek(topic, partitionOffsets) }
    //    }

    def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = TestIO { _.commit(offsets) }

    def poll() = TestIO { _.poll }

    def close() = TestIO { data => (data, ()) }
  }


  val journal: ReplicatedJournal[TestIO] = new ReplicatedJournal[TestIO] {

    def topics() = IO[TestIO].pure(Nil)

    def pointers(topic: Topic) = {
      // TODO TopicPointers.Empty
      TestIO { data => (data, data.pointers.getOrElse(topic, TopicPointers(Map.empty))) }
    }

    def append(key: Key, timestamp: Instant, events: Nel[ReplicatedEvent], deleteTo: Option[SeqNr]) = {
      TestIO { _.append(key, events, deleteTo) }
    }

    def delete(key: Key, timestamp: Instant, deleteTo: SeqNr, bound: Boolean) = {
      TestIO { _.delete(key, deleteTo, bound) }
    }

    def save(topic: Topic, pointers: TopicPointers) = {
      TestIO { _.save(topic, pointers) }
    }
  }


  val log: Log[TestIO] = Log.empty[TestIO](TestIO.TestIOIO.unit)


  val stopRef: Ref[Boolean, TestIO] = new Ref[Boolean, TestIO] {

    def set(value: Boolean) = TestIO { _.stop(value) }

    def get() = TestIO { data => (data, data.stopped) }
  }

  val topicReplicator: TopicReplicator[TestIO] = TopicReplicator(topic, partitions, consumer, journal, log, stopRef)


  // TODO create separate case class covering state of KafkaConsumer for testing
  final case class Data(
    topics: List[Topic] = Nil,
    commits: List[Map[TopicPartition, OffsetAndMetadata]] = Nil,
    records: List[ConsumerRecords[String, Bytes]] = Nil,
    stopped: Boolean = false,
    pointers: Map[Topic, TopicPointers] = Map.empty,
    journal: Map[Key, List[ReplicatedEvent]] = Map.empty,
    metadata: Map[Key, Option[Int]] = Map.empty) {
    self =>

    def subscribe(topic: Topic): (Data, Unit) = {
      val result = copy(topics = topic :: topics)
      (result, ())
    }

    def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): (Data, Unit) = {
      val result = copy(commits = offsets :: commits)
      (result, ())
    }

    /*def seek(topic: Topic, partitionOffsets: List[PartitionOffset]): (Data, Unit) = {
      val offsets = for {
        partitionOffset <- partitionOffsets
      } yield {
        (partitionOffset.partition, partitionOffset.offset)
      }
      val result = copy(subscription = Some((topic, offsets.toMap)))
      (result, ())
    }*/

    def stop(value: Boolean): (Data, Unit) = {
      //      (copy(stopped = value), ())
      (this, ())
    }

    def save(topic: Topic, pointers: TopicPointers): (Data, Unit) = {
      val updated = self.pointers.getOrElse(topic, TopicPointers.Empty) + pointers
      val result = copy(pointers = self.pointers.updated(topic, updated))
      (result, ())
    }

    def append(key: Key, events: Nel[ReplicatedEvent], deleteTo: Option[SeqNr]): (Data, Unit) = {

      val deletedToPrev = self.metadata.getOrElse(key, None)

      val (records, deletedTo) = {
        val records = self.journal.getOrElse(key, Nil)
        val head = deleteTo match {
          case Some(deleteTo) => records.dropWhile(_.seqNr <= deleteTo)
          case None           => records
        }
        (events.toList ++ head, deleteTo.map(_.value.toInt) orElse deletedToPrev)
      }

      val updated = copy(
        journal = journal.updated(key, records),
        metadata = metadata.updated(key, deletedTo))

      (updated, ())
    }

    def delete(key: Key, deleteTo: SeqNr, bound: Boolean): (Data, Unit) = {

      val head = self
        .journal.getOrElse(key, Nil)
        .dropWhile(_.seqNr <= deleteTo)

      val (records, deletedTo) = {
        if (bound) {
          (head, Some(deleteTo.value.toInt))
        } else {
          val deletedTo = head.headOption.flatMap(_.seqNr.prev)
          (head, deletedTo.map(_.value.toInt) orElse self.metadata.getOrElse(key, None))
        }
      }

      val updated = copy(
        journal = journal.updated(key, records),
        metadata = metadata.updated(key, deletedTo))

      (updated, ())
    }

    def poll: (Data, ConsumerRecords[String, Bytes]) = {
      records match {
        case head :: tail => (copy(records = tail), head)
        case Nil          => (copy(stopped = true), ConsumerRecords(Map.empty))
      }
    }
  }

  final case class TestIO[A](run: Data => (Data, A)) {
    self =>

    def map[B](ab: A => B): TestIO[B] = {
      TestIO { a => self.run(a) match { case (t, a) => (t, ab(a)) } }
    }

    def flatMap[B](afb: A => TestIO[B]): TestIO[B] = {
      TestIO { a => self.run(a) match { case (b, a) => afb(a).run(b) } }
    }

    def catchAll[B >: A](f: Throwable => TestIO[B]): TestIO[B] = {
      TestIO { data =>
        try self.run(data) catch { case NonFatal(failure) => f(failure).run(data) }
      }
    }
  }

  object TestIO {

    implicit val TestIOIO: IO[TestIO] = new IO[TestIO] {

      def pure[A](a: A) = TestIO { data => (data, a) }

      def point[A](a: => A) = TestIO { data => (data, a) }

      def flatMap[A, B](fa: TestIO[A], afb: A => TestIO[B]) = fa.flatMap(afb)

      def map[A, B](fa: TestIO[A], ab: A => B) = fa.map(ab)

      def unit[A] = TestIO { data => (data, ()) }

      def unit[A](fa: TestIO[A]) = fa.map(_ => ())

      def fold[A, S](iter: Iterable[A], s: S)(f: (S, A) => TestIO[S]) = {
        iter.foldLeft(pure(s)) { (s, a) => s.flatMap { s => f(s, a) } }
      }

      def foldWhile[S](s: S)(f: S => TestIO[Switch[S]]) = {

        def loop(s: S): TestIO[S] = for {
          switch <- f(s)
          s <- if (switch.stop) pure(switch.s) else loop(switch.s)
        } yield s

        loop(s)
      }

      def catchAll[A, B >: A](fa: TestIO[A], f: Throwable => TestIO[B]) = fa.catchAll(f)
    }
  }
}
