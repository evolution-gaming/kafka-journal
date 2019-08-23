package com.evolutiongaming.kafka.journal.util

import cats.data.{NonEmptyList => Nel}
import play.api.libs.json.{Format, Json}
import scodec.bits.ByteVector
import scodec.{Attempt, Codec, Err, codecs}

import scala.util.Try

object ScodecHelper {

  def nelCodec[A](codec: Codec[List[A]]): Codec[Nel[A]] = {
    val to = (a: List[A]) => {
      val nel = Nel.fromList(a)
      Attempt.fromOption(nel, Err("list is empty"))
    }
    val from = (a: Nel[A]) => Attempt.successful(a.toList)
    codec.exmap(to, from)
  }


  def formatCodec[A](implicit format: Format[A]): Codec[A] = {
    val fromBytes = (a: ByteVector) => {
      val jsValue = Try { Json.parse(a.toArray) }
      for {
        a <- Attempt.fromTry(jsValue)
        a <- format.reads(a).fold(a => Attempt.failure(Err(a.toString())), Attempt.successful)
      } yield a
    }
    val toBytes = (a: A) => {
      val jsValue = format.writes(a)
      val bytes = Json.toBytes(jsValue)
      val byteVector = ByteVector.view(bytes)
      Attempt.successful(byteVector)
    }
    codecs.bytes.exmap(fromBytes, toBytes)
  }
}
