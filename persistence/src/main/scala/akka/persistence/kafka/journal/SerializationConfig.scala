package akka.persistence.kafka.journal

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.{deriveEnumerationReader, deriveReader}

final case class SerializationConfig(jsonCodec: SerializationConfig.JsonCoded)

object SerializationConfig {

  val default: SerializationConfig = SerializationConfig(
    jsonCodec = JsonCoded.PlayJson
  )

  sealed trait JsonCoded

  object JsonCoded {
    case object PlayJson extends JsonCoded
    case object Jsoniter extends JsonCoded

    implicit val configReaderJsonCodec: ConfigReader[JsonCoded] = deriveEnumerationReader
  }

  implicit val configReaderSerialization: ConfigReader[SerializationConfig] = deriveReader
}
