package com.evolutiongaming.kafka.journal

import java.io.FileOutputStream

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal.FromBytes.implicits._
import com.evolutiongaming.kafka.journal.ToBytes.implicits._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scodec.bits.ByteVector

import scala.util.{Success, Try}

class EventsToBytesSpec extends AnyFunSuite with Matchers {

  def event(seqNr: Int, payload: Option[Payload] = None): Event = {
    val tags = (0 to seqNr).map(_.toString).toSet
    Event(SeqNr.unsafe(seqNr), tags, payload)
  }

  def event(seqNr: Int, payload: Payload): Event = {
    event(seqNr, payload.some)
  }

  def binary(a: String) = PayloadBinaryFromStr(a)

  for {
    (name, events) <- List(
      ("empty", Events(Nel.of(event(1)), Events.Metadata.empty/*TODO expiry: pass metadata*/)),
      ("binary", Events(Nel.of(event(1, binary("binary"))), Events.Metadata.empty/*TODO expiry: pass metadata*/)),
      ("text", Events(Nel.of(event(1, Payload.text("text"))), Events.Metadata.empty/*TODO expiry: pass metadata*/)),
      ("json", Events(Nel.of(event(1, Payload.json("json"))), Events.Metadata.empty/*TODO expiry: pass metadata*/)),
      ("empty-many", Events(
        Nel.of(
          event(1),
          event(2),
          event(3)),
        Events.Metadata.empty/*TODO expiry: pass metadata*/)),
      ("binary-many", Events(
        Nel.of(
          event(1, binary("1")),
          event(2, binary("2")),
          event(3, binary("3"))),
        Events.Metadata.empty/*TODO expiry: pass metadata*/)),
      ("text-many", Events(
        Nel.of(
          event(1, Payload.text("1")),
          event(2, Payload.text("2")),
          event(3, Payload.text("3"))),
        Events.Metadata.empty/*TODO expiry: pass metadata*/)),
      ("json-many", Events(
        Nel.of(
          event(1, Payload.json("1")),
          event(2, Payload.json("2")),
          event(3, Payload.json("3"))),
        Events.Metadata.empty/*TODO expiry: pass metadata*/)),
      ("empty-binary-text-json", Events(
        Nel.of(
          event(1),
          event(2, binary("binary")),
          event(3, Payload.text("text")),
          event(4, Payload.json("json"))),
        Events.Metadata.empty/*TODO expiry: pass metadata*/)))
  } {
    test(s"toBytes & fromBytes $name") {

      def verify(bytes: ByteVector) = {
        val actual = bytes.fromBytes[Try, Events]
        actual shouldEqual Success(events)
      }

      val bytes = events.toBytes[Try].get

      verify(bytes)

      verify(ByteVectorOf[Try](getClass, s"events-$name.bin").get)
    }
  }

  def writeToFile(bytes: ByteVector, path: String): Unit = {
    val os = new FileOutputStream(path)
    os.write(bytes.toArray)
    os.close()
  }
}