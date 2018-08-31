package com.evolutiongaming.kafka.journal

import java.lang.{Integer => IntJ, Long => LongJ}
import java.nio.ByteBuffer

import com.evolutiongaming.kafka.journal.Alias.Tag
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.serialization.SerializerHelper
import com.evolutiongaming.serialization.SerializerHelper._

import scala.annotation.tailrec

object EventsSerializer {

  def toBytes(events: Nel[Event]): Bytes = {

    def bytesOf(tags: Set[Tag]) = {
      if (tags.isEmpty) SerializerHelper.Bytes.Empty
      else {
        val bytes = tags.map(_.getBytes(Utf8))
        val length = bytes.foldLeft(0) { (length, bytes) => length + IntJ.BYTES + bytes.length }
        val buffer = ByteBuffer.allocate(length)
        bytes.foreach(buffer.writeBytes)
        buffer.array()
      }
    }

    val eventBytes = for {
      event <- events
    } yield {
      val payload = event.payload
      val tags = bytesOf(event.tags)
      val buffer = ByteBuffer.allocate(LongJ.BYTES + IntJ.BYTES + tags.length + IntJ.BYTES + payload.value.length)
      buffer.putLong(event.seqNr.value)
      buffer.writeBytes(tags)
      buffer.writeBytes(payload.value)
      buffer.array()
    }

    val length = eventBytes.foldLeft(IntJ.BYTES) { case (length, event) =>
      length + IntJ.BYTES + event.length
    }

    val buffer = ByteBuffer.allocate(length)
    buffer.writeNel(eventBytes)
    Bytes(buffer.array())
  }

  def fromBytes(bytes: Bytes): Nel[Event] = {
    val buffer = ByteBuffer.wrap(bytes.value)
    buffer.readNel {
      val seqNr = SeqNr(buffer.getLong())
      val tags = buffer.readTags()
      val payload = buffer.readBytes
      Event(seqNr, tags, Bytes(payload))
    }
  }


  implicit class ByteBufferNelOps(val self: ByteBuffer) extends AnyVal {

    def readNel[T](f: => T): Nel[T] = {
      val length = self.getInt()
      val list = List.fill(length) {
        val length = self.getInt
        val position = self.position()
        val value = f
        self.position(position + length)
        value
      }
      Nel.unsafe(list)
    }

    def writeNel(bytes: Nel[Array[Byte]]): Unit = {
      self.putInt(bytes.length)
      bytes.foreach { bytes => self.writeBytes(bytes) }
    }

    def readTags(): Set[Tag] = {
      val bytes = self.readBytes
      if (bytes.isEmpty) Set.empty
      else {
        val buffer = ByteBuffer.wrap(bytes)

        @tailrec
        def loop(tags: Set[Tag]): Set[Tag] = {
          if (!buffer.hasRemaining) tags
          else {
            val tag = buffer.readString
            loop(tags + tag)
          }
        }

        loop(Set.empty)
      }
    }
  }
}

