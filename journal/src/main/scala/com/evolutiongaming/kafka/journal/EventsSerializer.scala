package com.evolutiongaming.kafka.journal

import java.lang.{Integer => IntJ, Long => LongJ}
import java.nio.ByteBuffer

import com.evolutiongaming.nel.Nel
import com.evolutiongaming.serialization.SerializerHelper._

object EventsSerializer {

  def toBytes(events: Nel[Event]): Bytes = {

    val eventBytes = for {
      event <- events
    } yield {
      val payload = event.payload
      val buffer = ByteBuffer.allocate(LongJ.SIZE + IntJ.SIZE + payload.value.length)
      buffer.putLong(event.seqNr)
      buffer.writeBytes(payload.value)
      buffer.array()
    }

    val length = eventBytes.foldLeft(IntJ.SIZE) { case (length, event) =>
      length + IntJ.SIZE + event.length
    }

    val buffer = ByteBuffer.allocate(length)
    buffer.writeNel(eventBytes)
    Bytes(buffer.array())
  }

  def fromBytes(bytes: Bytes): Nel[Event] = {
    val buffer = ByteBuffer.wrap(bytes.value)
    buffer.readNel {
      val seqNr = buffer.getLong()
      val payload = buffer.readBytes
      Event(seqNr, Set.empty /*TODO*/ , Bytes(payload))
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
  }
}

