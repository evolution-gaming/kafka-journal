package com.evolutiongaming.kafka.journal

import com.evolutiongaming.jsonitertool.PlayJsonJsoniter
import play.api.libs.json.{JsError, JsResult, JsSuccess, JsValue, Json}
import scodec.bits.ByteVector

import scala.util.{Failure, Success, Try}

trait JsValueCodec {
  def encode(value: JsValue): ByteVector
  def decode(bytes: ByteVector): JsResult[JsValue]
}

object JsValueCodec {

  def apply()(implicit ev: JsValueCodec): JsValueCodec = ev

  def playJson: JsValueCodec = new JsValueCodec {

    def encode(value: JsValue): ByteVector =
      ByteVector(Json.toBytes(value))

    def decode(bytes: ByteVector): JsResult[JsValue] = toJsResult {
      Try(Json.parse(bytes.toArray))
    }

  }

  def jsoniter: JsValueCodec = new JsValueCodec {

    def encode(value: JsValue): ByteVector =
      ByteVector(PlayJsonJsoniter.serialize(value))

    def decode(bytes: ByteVector): JsResult[JsValue] = toJsResult {
      PlayJsonJsoniter.deserialize(bytes.toArray)
    }
  }

  object Implicits {
    implicit val default: JsValueCodec = JsValueCodec.playJson
  }

  @inline
  private def toJsResult(result: Try[JsValue]): JsResult[JsValue] =
    result match {
      case Success(value) => JsSuccess(value)
      case Failure(e)     => JsError(s"failed to parse json $e")
    }
}
