package com.evolutiongaming.kafka.journal

import java.lang.{Integer => IntJ}
import java.nio.ByteBuffer

import com.evolutiongaming.kafka.journal.FromBytes.Implicits._
import com.evolutiongaming.kafka.journal.ToBytes.Implicits._
import com.evolutiongaming.kafka.journal.util.ByteBufferHelper._

import scala.annotation.tailrec

object Tags {

  val Empty: Tags = Set.empty

  implicit val TagsToBytes: ToBytes[Tags] = new ToBytes[Tags] {
    def apply(tags: Tags) = {
      if (tags.isEmpty) Bytes.Empty
      else {
        val bytes = tags.map(_.toBytes)
        val length = bytes.foldLeft(0) { (length, bytes) => length + IntJ.BYTES + bytes.length }
        val buffer = ByteBuffer.allocate(length)
        bytes.foreach(buffer.writeBytes)
        buffer.array()
      }
    }
  }

  implicit val TagsFromBytes: FromBytes[Tags] = new FromBytes[Tags] {
    def apply(bytes: Bytes) = {
      if (bytes.isEmpty) Set.empty
      else {
        val buffer = ByteBuffer.wrap(bytes)

        @tailrec
        def loop(tags: Tags): Tags = {
          if (!buffer.hasRemaining) tags
          else {
            val tag = buffer.readBytes.fromBytes[Tag]
            loop(tags + tag)
          }
        }

        loop(Set.empty)
      }
    }
  }

  def apply(tag: Tag, tags: Tag*): Tags = tags.toSet[Tag] + tag
}