package com.evolutiongaming.kafka.journal

import java.io.FileOutputStream

import com.evolutiongaming.kafka.journal.EventsSerializer._
import com.evolutiongaming.kafka.journal.FixEquality.Implicits._
import com.evolutiongaming.kafka.journal.FromBytes.Implicits._
import com.evolutiongaming.kafka.journal.ToBytes.Implicits._
import com.evolutiongaming.nel.Nel
import org.scalatest.{FunSuite, Matchers}

class EventsToBytesSpec extends FunSuite with Matchers {

  private implicit val fixEquality = FixEquality.array[Byte]()

  def event(seqNr: Int, payload: Option[Payload] = None): Event = {
    val tags = (0 to seqNr).map(_.toString).toSet
    Event(SeqNr(seqNr.toLong), tags, payload)
  }

  def event(seqNr: Int, payload: Payload): Event = {
    event(seqNr, Some(payload))
  }

  for {
    (name, events) <- List(
      ("empty", Nel(event(1))),
      ("binary", Nel(event(1, Payload.binary("binary")))),
      ("text", Nel(event(1, Payload.text("text")))),
      ("json", Nel(event(1, Payload.json("json")))),
      ("empty-many", Nel(
        event(1),
        event(2),
        event(3))),
      ("binary-many", Nel(
        event(1, Payload.binary("1")),
        event(2, Payload.binary("2")),
        event(3, Payload.binary("3")))),
      ("text-many", Nel(
        event(1, Payload.text("1")),
        event(2, Payload.text("2")),
        event(3, Payload.text("3")))),
      ("json-many", Nel(
        event(1, Payload.json("1")),
        event(2, Payload.json("2")),
        event(3, Payload.json("3")))),
      ("empty-binary-text-json", Nel(
        event(1),
        event(2, Payload.binary("binary")),
        event(3, Payload.text("text")),
        event(4, Payload.json("json")))))
  } {
    test(s"toBytes & fromBytes $name") {

      def verify(bytes: Bytes) = {
        val actual = bytes.fromBytes[Nel[Event]]
        actual.fix shouldEqual events.fix
      }

      val bytes = events.toBytes

      verify(bytes)

      verify(BytesOf(getClass, s"events-$name.bin"))
    }
  }

  def writeToFile(bytes: Bytes, path: String): Unit = {
    val os = new FileOutputStream(path)
    os.write(bytes)
    os.close()
  }
}