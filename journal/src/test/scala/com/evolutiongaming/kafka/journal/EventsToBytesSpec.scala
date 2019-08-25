package com.evolutiongaming.kafka.journal

import java.io.FileOutputStream

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.kafka.journal.FromBytes.Implicits._
import com.evolutiongaming.kafka.journal.ToBytes.Implicits._
import org.scalatest.{FunSuite, Matchers}
import scodec.bits.ByteVector

import scala.util.Try

class EventsToBytesSpec extends FunSuite with Matchers {

  def event(seqNr: Int, payload: Option[Payload] = None): Event = {
    val tags = (0 to seqNr).map(_.toString).toSet
    Event(SeqNr(seqNr.toLong), tags, payload)
  }

  def event(seqNr: Int, payload: Payload): Event = {
    event(seqNr, Some(payload))
  }

  def binary(a: String) = PayloadBinaryFromStr(a)

  for {
    (name, events) <- List(
      ("empty", Nel.of(event(1))),
      ("binary", Nel.of(event(1, binary("binary")))),
      ("text", Nel.of(event(1, Payload.text("text")))),
      ("json", Nel.of(event(1, Payload.json("json")))),
      ("empty-many", Nel.of(
        event(1),
        event(2),
        event(3))),
      ("binary-many", Nel.of(
        event(1, binary("1")),
        event(2, binary("2")),
        event(3, binary("3")))),
      ("text-many", Nel.of(
        event(1, Payload.text("1")),
        event(2, Payload.text("2")),
        event(3, Payload.text("3")))),
      ("json-many", Nel.of(
        event(1, Payload.json("1")),
        event(2, Payload.json("2")),
        event(3, Payload.json("3")))),
      ("empty-binary-text-json", Nel.of(
        event(1),
        event(2, binary("binary")),
        event(3, Payload.text("text")),
        event(4, Payload.json("json")))))
  } {
    test(s"toBytes & fromBytes $name") {

      def verify(bytes: ByteVector) = {
        val actual = bytes.toArray.fromBytes[Nel[Event]]
        actual shouldEqual events
      }

      val bytes = events.toBytes[Try].get

      verify(bytes)

      verify(ByteVectorOf(getClass, s"events-$name.bin"))
    }
  }

  def writeToFile(bytes: ByteVector, path: String): Unit = {
    val os = new FileOutputStream(path)
    os.write(bytes.toArray)
    os.close()
  }
}