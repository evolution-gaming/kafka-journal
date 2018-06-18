package com.evolutiongaming.kafka.journal

import java.lang.{Integer => IntJ, Long => LongJ}
import java.nio.ByteBuffer

import com.evolutiongaming.kafka.journal.JournalRecord.Payload
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.serialization.SerializerHelper._
import com.evolutiongaming.skafka.{FromBytes, ToBytes, Topic}

object EventsSerializer {

  implicit val EventsToBytes: ToBytes[JournalRecord.Payload.Events] = new ToBytes[JournalRecord.Payload.Events] {

    def apply(value: Payload.Events, topic: Topic) = {

      val events = for {event <- value.events} yield {
        val payload = event.payload
        val buffer = ByteBuffer.allocate(LongJ.SIZE + IntJ.SIZE + payload.length)
        buffer.putLong(event.seqNr)
        buffer.writeBytes(payload)
        buffer.array()
      }

      val length = events.foldLeft(IntJ.SIZE) { case (length, event) =>
        length + IntJ.SIZE + event.length
      }

      val buffer = ByteBuffer.allocate(length)
      buffer.writeNel(events)
      buffer.array()
    }
  }

  implicit val EventsFromBytes: FromBytes[JournalRecord.Payload.Events] = new FromBytes[JournalRecord.Payload.Events] {

    def apply(bytes: Bytes, topic: Topic) = {
      val buffer = ByteBuffer.wrap(bytes)
      val events = buffer.readNel {
        val seqNr = buffer.getLong()
        val payload = buffer.readBytes
        JournalRecord.Event(seqNr, payload)
      }
      JournalRecord.Payload.Events(events)
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

