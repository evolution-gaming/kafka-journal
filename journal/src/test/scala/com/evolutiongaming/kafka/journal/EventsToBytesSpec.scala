package com.evolutiongaming.kafka.journal

import java.io.FileOutputStream

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.kafka.journal.FromBytes.implicits._
import com.evolutiongaming.kafka.journal.ToBytes.implicits._
import org.scalatest.{FunSuite, Matchers}
import scodec.bits.ByteVector

import scala.util.{Success, Try}

class EventsToBytesSpec extends FunSuite with Matchers {

  def event(seqNr: Int, payload: Option[Payload] = None): Event = {
    val tags = (0 to seqNr).map(_.toString).toSet
    Event(SeqNr.unsafe(seqNr), tags, payload)
  }

  def event(seqNr: Int, payload: Payload): Event = {
    event(seqNr, Some(payload))
  }

  def binary(a: String) = PayloadBinaryFromStr(a)

  for {
    (name, events) <- List(
      ("empty", Events(Nel.of(event(1)))),
      ("binary", Events(Nel.of(event(1, binary("binary"))))),
      ("text", Events(Nel.of(event(1, Payload.text("text"))))),
      ("json", Events(Nel.of(event(1, Payload.json("json"))))),
      ("empty-many", Events(Nel.of(
        event(1),
        event(2),
        event(3)))),
      ("binary-many", Events(Nel.of(
        event(1, binary("1")),
        event(2, binary("2")),
        event(3, binary("3"))))),
      ("text-many", Events(Nel.of(
        event(1, Payload.text("1")),
        event(2, Payload.text("2")),
        event(3, Payload.text("3"))))),
      ("json-many", Events(Nel.of(
        event(1, Payload.json("1")),
        event(2, Payload.json("2")),
        event(3, Payload.json("3"))))),
      ("empty-binary-text-json", Events(Nel.of(
        event(1),
        event(2, binary("binary")),
        event(3, Payload.text("text")),
        event(4, Payload.json("json"))))))
  } {
    test(s"toBytes & fromBytes $name") {

      def verify(bytes: ByteVector) = {
        val actual = bytes.fromBytes[Try, Events]
        actual shouldEqual Success(events)
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