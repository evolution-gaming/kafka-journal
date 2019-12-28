package com.evolutiongaming.kafka.journal

import java.io.FileOutputStream

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal.FromBytes.implicits._
import com.evolutiongaming.kafka.journal.ToBytes.implicits._
import com.evolutiongaming.kafka.journal.ExpireAfter.implicits._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.Json
import scodec.bits.ByteVector

import scala.util.Try
import scala.concurrent.duration._

class EventsToBytesSpec extends AnyFunSuite with Matchers {

  def event(seqNr: Int, payload: Option[Payload] = None): Event = {
    val tags = (0 to seqNr).map(_.toString).toSet
    Event(SeqNr.unsafe(seqNr), tags, payload)
  }

  def event(seqNr: Int, payload: Payload): Event = {
    event(seqNr, payload.some)
  }

  def binary(a: String): Payload = PayloadBinaryFromStr(a)

  private val payloadMetadata = PayloadMetadata(
    1.day.toExpireAfter.some,
    Json.obj(("key", "value")).some)

  for {
    (name, events) <- List(
      ("empty", Events(
        Nel.of(
          event(1)),
        PayloadMetadata.empty)),
      ("binary", Events(
        Nel.of(
          event(1, binary("binary"))),
        PayloadMetadata.empty)),
      ("text", Events(
        Nel.of(
          event(1, Payload.text("text"))),
        PayloadMetadata.empty)),
      ("json", Events(
        Nel.of(
          event(1, Payload.json("json"))),
        payloadMetadata)),
      ("empty-many", Events(
        Nel.of(
          event(1),
          event(2),
          event(3)),
        payloadMetadata)),
      ("binary-many", Events(
        Nel.of(
          event(1, binary("1")),
          event(2, binary("2")),
          event(3, binary("3"))),
        payloadMetadata)),
      ("text-many", Events(
        Nel.of(
          event(1, Payload.text("1")),
          event(2, Payload.text("2")),
          event(3, Payload.text("3"))),
        payloadMetadata)),
      ("json-many", Events(
        Nel.of(
          event(1, Payload.json("1")),
          event(2, Payload.json("2")),
          event(3, Payload.json("3"))),
        payloadMetadata)),
      ("empty-binary-text-json", Events(
        Nel.of(
          event(1),
          event(2, binary("binary")),
          event(3, Payload.text("text")),
          event(4, Payload.json("json"))),
        payloadMetadata)))
  } {
    test(s"toBytes & fromBytes $name") {

      def verify(bytes: ByteVector) = {
        val actual = bytes.fromBytes[Try, Events]
        actual shouldEqual events.pure[Try]
      }

      val result = for {
        bytes <- events.toBytes[Try]
//        _ = writeToFile(bytes, s"v1-events-$name.bin")
        _      = verify(bytes)
        bytes <- ByteVectorOf[Try](getClass, s"v1-events-$name.bin")
        _      = verify(bytes)
      } yield {}
      result shouldEqual ().pure[Try]
    }
  }

  for {
    (name, events) <- List(
      ("empty", Events(
        Nel.of(
          event(1)),
        PayloadMetadata.empty)),
      ("binary", Events(
        Nel.of(
          event(1, binary("binary"))),
        PayloadMetadata.empty)),
      ("text", Events(
        Nel.of(
          event(1, Payload.text("text"))),
        PayloadMetadata.empty)),
      ("json", Events(
        Nel.of(
          event(1, Payload.json("json"))),
        PayloadMetadata.empty)),
      ("empty-many", Events(
        Nel.of(
          event(1),
          event(2),
          event(3)),
        PayloadMetadata.empty)),
      ("binary-many", Events(
        Nel.of(
          event(1, binary("1")),
          event(2, binary("2")),
          event(3, binary("3"))),
        PayloadMetadata.empty)),
      ("text-many", Events(
        Nel.of(
          event(1, Payload.text("1")),
          event(2, Payload.text("2")),
          event(3, Payload.text("3"))),
        PayloadMetadata.empty)),
      ("json-many", Events(
        Nel.of(
          event(1, Payload.json("1")),
          event(2, Payload.json("2")),
          event(3, Payload.json("3"))),
        PayloadMetadata.empty)),
      ("empty-binary-text-json", Events(
        Nel.of(
          event(1),
          event(2, binary("binary")),
          event(3, Payload.text("text")),
          event(4, Payload.json("json"))),
        PayloadMetadata.empty)))
  } {
    test(s"fromBytes $name") {
      val actual = for {
        bytes  <- ByteVectorOf[Try](getClass, s"v0-events-$name.bin")
        events <- bytes.fromBytes[Try, Events]
      } yield events

      actual shouldEqual events.pure[Try]
    }
  }

  def writeToFile(bytes: ByteVector, path: String): Unit = {
    val os = new FileOutputStream(path)
    os.write(bytes.toArray)
    os.close()
  }
}