package com.evolutiongaming.kafka.journal

import java.util.UUID

import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.EventsSerializer._
import com.evolutiongaming.kafka.journal.ally.{AllyDbRead, PartitionOffset}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerRecord}
import com.evolutiongaming.skafka.producer.{Producer, ProducerRecord}
import com.evolutiongaming.skafka.{Header, TopicPartition}
import com.evolutiongaming.util.FutureHelper._
import play.api.libs.json.Json

import scala.annotation.tailrec
import scala.collection.immutable.Seq
import scala.compat.Platform
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

trait Client {
  def append(id: Id, events: Nel[Entry]): Future[Unit]
  // TODO decide on return type
  def read(id: Id, range: SeqRange): Future[Seq[Entry]]
  def lastSeqNr(id: Id, from: SeqNr): Future[SeqNr]
  def truncate(id: Id, to: SeqNr): Future[Unit]
}

object Client {

  val Empty: Client = new Client {
    def append(id: Id, events: Nel[Entry]) = Future.unit
    def read(id: Id, range: SeqRange): Future[List[Entry]] = Future.successful(Nil)
    def lastSeqNr(id: Id, from: SeqNr) = Future.successful(0L)
    def truncate(id: Id, to: SeqNr) = Future.unit
  }

  def apply(settings: Settings): Client = ???

  def apply(
    producer: Producer,
    newConsumer: () => Consumer[String, Array[Byte]],
    allyDb: AllyDbRead = AllyDbRead.Empty,
    pollTimeout: FiniteDuration = 100.millis)(implicit ec: ExecutionContext): Client = {

    def toTopic(id: Id) = "journal-test9"

    def toHeader(a: Action) = {
      val json = Json.toJson(a)
      val bytes = Json.toBytes(json)
      Header("journal.action", bytes)
    }

    def toAction(record: ConsumerRecord[String, Array[Byte]]) = {
      val headers = record.headers
      val header = headers.find { _.key == "journal.action" }.get
      val json = Json.parse(header.value)
      json.as[Action]
    }

    case class Result[T](value: T, stop: Boolean)

    def consume[T](id: Id, initial: T, partitionOffset: Option[PartitionOffset])(f: (T, ConsumerRecord[String, Array[Byte]]) => Result[T]): T = {
      val consumer = newConsumer()

      @tailrec def consume(initial: T): T = {
        val records = consumer.poll(pollTimeout)
        val zero = Result(initial, stop = false)
        val result = records.values.foldLeft(zero) { case (result, (_, records)) =>
          if (result.stop) result
          else records.foldLeft(result) { case (result, record) =>
            if (result.stop) result
            else if (record.key contains id) f(result.value, record)
            else result
          }
        }

        if (result.stop) {
          consumer.close() // TODO
          result.value
        } else {
          consume(result.value)
        }
      }

      val topic = toTopic(id)
      val topics = List(topic)

      partitionOffset match {
        case None =>
          consumer.subscribe(topics) // TODO with listener
        //          consumer.seekToBeginning()

        case Some(partitionOffset) =>
          val topicPartition = TopicPartition(topic, partitionOffset.partition)
          consumer.assign(List(topicPartition)) // TODO blocking
          consumer.seek(topicPartition, partitionOffset.offset) // TODO blocking
      }

      //      partitionOffset.foreach { partitionOffset =>
      //        val topicPartition = TopicPartition(topic, partitionOffset.partition)
      //        consumer.seek(topicPartition, partitionOffset.offset)
      //      }
      consume(initial)
    }

    def consumeActions[T](
      id: Id,
      partitionOffset: Option[PartitionOffset],
      initial: T)(f: (T, ConsumerRecord[String, Array[Byte]], Action.AppendOrTruncate) => T): Future[T] = {

      val ecBlocking = ec // TODO

      val topic = toTopic(id)
      val readId = UUID.randomUUID().toString
      val action = Action.Mark(readId)
      val header = toHeader(action)
      val record = ProducerRecord(
        topic = topic,
        value = Array.empty[Byte],
        key = Some(id),
        headers = List(header))

      val write = producer(record)

      val read = Future {
        val seqNr = consume(id, initial, partitionOffset) { case (seqNr, record) =>
          val a = toAction(record)
          a match {
            case a: Action.AppendOrTruncate =>
              val result = f(seqNr, record, a)
              Result(result, stop = false)

            case a: Action.Mark =>
              val stop = a.id == readId
              Result(seqNr, stop)
          }
        }
        seqNr
      }(ecBlocking)

      for {
        _ <- write
        r <- read
      } yield r
    }

    new Client {

      def append(id: Id, events: Nel[Entry]): Future[Unit] = {

        val timestamp = Platform.currentTime // TODO argument
        val events2 = for {
          event <- events
        } yield {
          JournalRecord.Event(event.seqNr, event.payload)
        }

        val payload = JournalRecord.Payload.Events(events2)
        val topic = toTopic(id)
        val range = SeqRange(from = events.head.seqNr, to = events.last.seqNr)
        val action = Action.Append(range)
        val header = toHeader(action)
        val record = ProducerRecord(
          topic = topic,
          value = payload,
          key = Some(id),
          timestamp = Some(timestamp),
          headers = List(header))

        val result = producer(record)
        result.unit
      }

      def read(id: Id, range: SeqRange): Future[Seq[Entry]] = {

        println(s"$id Client.read range: $range")

        val topic = toTopic(id) // TODO another topic created inside of consumeActions

        // write marker
        // query for last
        // query events and start
        // start consuming


        def allyRecords() = {
          for {
            allyRecords <- allyDb.list(id, range)
          } yield {
            allyRecords.map { record =>
              Entry(
                payload = record.payload,
                seqNr = record.seqNr,
                tags = record.tags)
            }
          }
        }

        case class Result(deleteTo: SeqNr, entries: Vector[Entry])

        val zero = Result(0, Vector.empty)


        val result = for {
          record <- allyDb.last(id, range.from)
          partitionOffset = record.map { _.partitionOffset }

          entries <- allyRecords()

          // TODO use range after allyRecords
          records <- consumeActions(id, partitionOffset, zero) { case (result, record, a) =>
            a match {
              case action: Action.Append =>
                val bytes = record.value

                def entries = {
                  EventsFromBytes(bytes, topic)
                    .events
                    .to[Vector]
                    .map { event =>
                      val tags = Set.empty[String] // TODO
                      Entry(event.payload, event.seqNr, tags)
                    }
                }

                if (range.contains(action.range)) {
                  // TODO we don't need to deserialize entries that are out of scope
                  result.copy(entries = result.entries ++ entries)

                } else if (action.range < range) {
                  result
                } else if (action.range > range) {
                  // TODO stop consuming
                  result
                } else {

                  val filtered = entries.filter { entry => range contains entry.seqNr }

                  if (entries.last.seqNr > range) {
                    // TODO stop consuming
                    result.copy(entries = result.entries ++ filtered)
                  } else {
                    result.copy(entries = result.entries ++ filtered)
                  }
                }

              case a: Action.Truncate =>
                if (a.to > result.deleteTo) {
                  val entries = result.entries.dropWhile(_.seqNr <= a.to)
                  result.copy(deleteTo = a.to, entries = entries)
                } else {
                  result
                }
            }
          }
        } yield {

          val cassandraEntries = entries.dropWhile(_.seqNr <= records.deleteTo)

          cassandraEntries.lastOption match {
            case None =>

              // TODO important performance indication
              println(s"$id >>>> ${ records.entries.size } <<<<")
              records.entries

            case Some(last) =>
              val kafka = records.entries.dropWhile { _.seqNr <= last.seqNr }
              // TODO important performance indication
              println(s"$id >>>> ${ kafka.size }(${{cassandraEntries.size}}) <<<<")
              cassandraEntries ++ kafka
          }
        }

        result.failed.foreach { failure =>
          failure.printStackTrace()
        }

        result
      }

      def lastSeqNr(id: Id, from: SeqNr) = {
        for {
          record <- allyDb.last(id, from)
          partitionOffset = record.map { _.partitionOffset }
          result <- consumeActions(id, partitionOffset, 0L) { case (seqNr, _, a) =>
            a match {
              case a: Action.Append   => a.range.to
              case a: Action.Truncate => seqNr
            }
          }
        } yield {
          result
        }
      }

      def truncate(id: Id, to: SeqNr): Future[Unit] = {
        val timestamp = Platform.currentTime // TODO argument
        val json = Json.obj("seqNr" -> to)
        val payload = Json.toBytes(json)
        val topic = toTopic(id)
        val a = Action.Truncate(to)
        val header = toHeader(a)
        val record = ProducerRecord(
          topic = topic,
          value = payload,
          key = Some(id),
          timestamp = Some(timestamp),
          headers = List(header))
        val result = producer(record)
        result.map { _ => () }
      }
    }
  }
}

// TODO timestamp ?
case class Entry(payload: Array[Byte], seqNr: SeqNr, tags: Set[String])
