package com.evolutiongaming.kafka.journal

import java.util.UUID

import com.evolutiongaming.kafka.journal.Aliases._
import com.evolutiongaming.kafka.journal.EventsSerializer._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Header
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerRecord}
import com.evolutiongaming.skafka.producer.Producer
import com.evolutiongaming.skafka.producer.ProducerRecord
import com.evolutiongaming.util.FutureHelper._
import play.api.libs.json.Json

import scala.annotation.tailrec
import scala.compat.Platform
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

trait Client {
  def append(id: Id, events: Nel[Entry]): Future[Unit]
  def read(id: Id): Future[List[Entry]]
  def lastSeqNr(id: Id): Future[SeqNr]
  def truncate(id: Id, to: SeqNr): Future[Unit]
}

object Client {

  val Empty: Client = new Client {
    def append(id: Id, events: Nel[Entry]) = Future.unit
    def read(id: Id): Future[List[Entry]] = Future.successful(Nil)
    def lastSeqNr(id: Id) = Future.successful(0L)
    def truncate(id: Id, to: SeqNr) = Future.unit
  }

  def apply(settings: Settings): Client = ???

  def apply(
    producer: Producer,
    newConsumer: () => Consumer[String, Array[Byte]],
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
      json.validate[Action].get
    }

    case class Result[T](value: T, stop: Boolean)

    def consume[T](id: Id, initial: T)(f: (T, ConsumerRecord[String, Array[Byte]]) => Result[T]): T = {
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
      consumer.subscribe(topics)
      consume(initial)
    }

    def consumeActions[T](id: Id, initial: T)(f: (T, ConsumerRecord[String, Array[Byte]], Action.AppendOrTruncate) => T): Future[T] = {
      val ecBlocking = ec // TODO

      val topic = toTopic(id)
      val readId = UUID.randomUUID().toString
      val action = Action.Read(readId)
      val header = toHeader(action)
      val record = ProducerRecord(
        topic = topic,
        value = Array.empty[Byte],
        key = Some(id),
        headers = List(header))

      val write = producer(record)

      val read = Future {
        val seqNr = consume(id, initial) { case (seqNr, record) =>
          val a = toAction(record)
          a match {
            case a: Action.AppendOrTruncate =>
              val result = f(seqNr, record, a)
              Result(result, stop = false)

            case a: Action.Read =>
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
        val action = Action.Append(from = events.head.seqNr, to = events.last.seqNr)
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

      def read(id: Id): Future[List[Entry]] = {

        consumeActions(id, List.empty[Entry]) { case (events, record, a) =>
          a match {
            case a: Action.Append =>
              val bytes = record.value
              val xs = EventsFromBytes(bytes)
              val head = xs.events.map { event =>
                val tags = Set.empty[String] // TODO
                Entry(event.payload, event.seqNr, tags)
              }
              head.toList ::: events

            case a: Action.Truncate =>
              events.dropWhile(_.seqNr <= a.to)
          }
        }
      }

      def lastSeqNr(id: Id) = {
        consumeActions(id, 0L) { case (seqNr, record, a) =>
          a match {
            case a: Action.Append   => a.to
            case a: Action.Truncate => seqNr
          }
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
