package com.evolution.kafka.journal

import com.evolution.playjson.jsoniter.PlayJsonJsoniter
import com.github.plokhotnyuk.jsoniter_scala.core.{JsonReader, JsonValueCodec, JsonWriter, readFromArray}
import play.api.libs.json.{JsNull, JsValue}

/**
 * Decodes the JSON of [[ActionHeader]], as written by the journal, straight from bytes, without
 * building a `JsValue` tree first.
 *
 * Accepts the same JSON as [[ActionHeader.formatOptActionHeader]] where the result is guaranteed to
 * be the same, and fails on anything else, such as duplicate keys, more than one action, `null`
 * instead of an omitted field, or numbers written as decimals. The caller is expected to fall back
 * to [[ActionHeader.formatOptActionHeader]] on failure, which reads such JSON or reports a proper
 * error.
 */
private[journal] object ActionHeaderJson {

  /**
   * @throws com.github.plokhotnyuk.jsoniter_scala.core.JsonReaderException
   *   or another exception if the JSON is not in the expected form.
   */
  def decode(bytes: Array[Byte]): Option[ActionHeader] = readFromArray(bytes)(Codec)

  private final class OptActionHeaderCodec extends JsonValueCodec[Option[ActionHeader]] {

    def nullValue: Option[ActionHeader] = None

    def encodeValue(x: Option[ActionHeader], out: JsonWriter): Unit =
      throw new UnsupportedOperationException("ActionHeaderJson only decodes")

    def decodeValue(in: JsonReader, default: Option[ActionHeader]): Option[ActionHeader] = {
      if (!in.isNextToken('{')) in.decodeError("expected '{'")
      var result: ActionHeader = null
      if (!in.isNextToken('}')) {
        in.rollbackToken()
        var l = -1
        while (l < 0 || in.isNextToken(',')) {
          l = in.readKeyAsCharBuf()
          val header =
            if (in.isCharBufEqualsTo(l, "append")) append(in)
            else if (in.isCharBufEqualsTo(l, "mark")) mark(in)
            else if (in.isCharBufEqualsTo(l, "delete")) delete(in)
            else if (in.isCharBufEqualsTo(l, "purge")) purge(in)
            else {
              in.skip()
              null
            }
          if (header ne null) {
            // play-json reads the first one by precedence, not by position
            if (result ne null) in.decodeError("more than one action")
            result = header
          }
        }
        if (!in.isCurrentToken('}')) in.objectEndOrCommaError()
      }
      if (result eq null) None else Some(result)
    }
  }

  private val Codec: JsonValueCodec[Option[ActionHeader]] = new OptActionHeaderCodec

  private final val RangeBit = 1
  private final val OriginBit = 2
  private final val VersionBit = 4
  private final val PayloadTypeBit = 8
  private final val MetadataBit = 16
  private final val IdBit = 32
  private final val ToBit = 64

  private def append(in: JsonReader): ActionHeader = {
    objectStart(in)
    var seen = 0
    var range: SeqRange = null
    var origin: String = null
    var version: String = null
    var payloadType: PayloadType.BinaryOrJson = null
    var metadata = HeaderMetadata.empty
    if (!in.isNextToken('}')) {
      in.rollbackToken()
      var l = -1
      while (l < 0 || in.isNextToken(',')) {
        l = in.readKeyAsCharBuf()
        if (in.isCharBufEqualsTo(l, "range")) {
          seen = markSeen(in, seen, RangeBit)
          range = seqRange(in)
        } else if (in.isCharBufEqualsTo(l, "origin")) {
          seen = markSeen(in, seen, OriginBit)
          origin = in.readString(null)
        } else if (in.isCharBufEqualsTo(l, "version")) {
          seen = markSeen(in, seen, VersionBit)
          version = in.readString(null)
        } else if (in.isCharBufEqualsTo(l, "payloadType")) {
          seen = markSeen(in, seen, PayloadTypeBit)
          payloadType = binaryOrJson(in)
        } else if (in.isCharBufEqualsTo(l, "metadata")) {
          seen = markSeen(in, seen, MetadataBit)
          metadata = headerMetadata(in)
        } else {
          in.skip()
        }
      }
      if (!in.isCurrentToken('}')) in.objectEndOrCommaError()
    }
    if ((range eq null) || (payloadType eq null)) in.decodeError("missing range or payloadType")
    // a missing metadata is read as empty, see `ActionHeader.formatOptActionHeader`
    ActionHeader.Append(range, originOf(origin), versionOf(version), payloadType, metadata)
  }

  private def mark(in: JsonReader): ActionHeader = {
    objectStart(in)
    var seen = 0
    var id: String = null
    var origin: String = null
    var version: String = null
    if (!in.isNextToken('}')) {
      in.rollbackToken()
      var l = -1
      while (l < 0 || in.isNextToken(',')) {
        l = in.readKeyAsCharBuf()
        if (in.isCharBufEqualsTo(l, "id")) {
          seen = markSeen(in, seen, IdBit)
          id = in.readString(null)
        } else if (in.isCharBufEqualsTo(l, "origin")) {
          seen = markSeen(in, seen, OriginBit)
          origin = in.readString(null)
        } else if (in.isCharBufEqualsTo(l, "version")) {
          seen = markSeen(in, seen, VersionBit)
          version = in.readString(null)
        } else {
          in.skip()
        }
      }
      if (!in.isCurrentToken('}')) in.objectEndOrCommaError()
    }
    if (id eq null) in.decodeError("missing id")
    ActionHeader.Mark(id, originOf(origin), versionOf(version))
  }

  private def delete(in: JsonReader): ActionHeader = {
    objectStart(in)
    var seen = 0
    var to: SeqNr = null
    var origin: String = null
    var version: String = null
    if (!in.isNextToken('}')) {
      in.rollbackToken()
      var l = -1
      while (l < 0 || in.isNextToken(',')) {
        l = in.readKeyAsCharBuf()
        if (in.isCharBufEqualsTo(l, "to")) {
          seen = markSeen(in, seen, ToBit)
          to = SeqNr.unsafe(in.readLong())
        } else if (in.isCharBufEqualsTo(l, "origin")) {
          seen = markSeen(in, seen, OriginBit)
          origin = in.readString(null)
        } else if (in.isCharBufEqualsTo(l, "version")) {
          seen = markSeen(in, seen, VersionBit)
          version = in.readString(null)
        } else {
          in.skip()
        }
      }
      if (!in.isCurrentToken('}')) in.objectEndOrCommaError()
    }
    if (to eq null) in.decodeError("missing to")
    ActionHeader.Delete(DeleteTo(to), originOf(origin), versionOf(version))
  }

  private def purge(in: JsonReader): ActionHeader = {
    objectStart(in)
    var seen = 0
    var origin: String = null
    var version: String = null
    if (!in.isNextToken('}')) {
      in.rollbackToken()
      var l = -1
      while (l < 0 || in.isNextToken(',')) {
        l = in.readKeyAsCharBuf()
        if (in.isCharBufEqualsTo(l, "origin")) {
          seen = markSeen(in, seen, OriginBit)
          origin = in.readString(null)
        } else if (in.isCharBufEqualsTo(l, "version")) {
          seen = markSeen(in, seen, VersionBit)
          version = in.readString(null)
        } else {
          in.skip()
        }
      }
      if (!in.isCurrentToken('}')) in.objectEndOrCommaError()
    }
    ActionHeader.Purge(originOf(origin), versionOf(version))
  }

  private def seqRange(in: JsonReader): SeqRange = {
    objectStart(in)
    var seen = 0
    var from = 0L
    var to = 0L
    if (!in.isNextToken('}')) {
      in.rollbackToken()
      var l = -1
      while (l < 0 || in.isNextToken(',')) {
        l = in.readKeyAsCharBuf()
        if (in.isCharBufEqualsTo(l, "from")) {
          seen = markSeen(in, seen, 1)
          from = in.readLong()
        } else if (in.isCharBufEqualsTo(l, "to")) {
          seen = markSeen(in, seen, 2)
          to = in.readLong()
        } else {
          in.skip()
        }
      }
      if (!in.isCurrentToken('}')) in.objectEndOrCommaError()
    }
    if (seen != 3) in.decodeError("missing from or to")
    SeqRange(SeqNr.unsafe(from), SeqNr.unsafe(to))
  }

  private def headerMetadata(in: JsonReader): HeaderMetadata = {
    objectStart(in)
    var seen = 0
    var data: JsValue = JsNull
    if (!in.isNextToken('}')) {
      in.rollbackToken()
      var l = -1
      while (l < 0 || in.isNextToken(',')) {
        l = in.readKeyAsCharBuf()
        if (in.isCharBufEqualsTo(l, "data")) {
          seen = markSeen(in, seen, 1)
          data = PlayJsonJsoniter.jsValueCodec.decodeValue(in, JsNull)
        } else {
          in.skip()
        }
      }
      if (!in.isCurrentToken('}')) in.objectEndOrCommaError()
    }
    // play-json reads `null` of an optional field as `None`
    if (data == JsNull) HeaderMetadata.empty else HeaderMetadata(Some(data))
  }

  /**
   * Compares the name in the reader's buffer, without allocating a string.
   */
  private def binaryOrJson(in: JsonReader): PayloadType.BinaryOrJson = {
    val l = in.readStringAsCharBuf()
    if (in.isCharBufEqualsTo(l, PayloadType.Json.name)) PayloadType.Json
    else if (in.isCharBufEqualsTo(l, PayloadType.Binary.name)) PayloadType.Binary
    else in.decodeError("unexpected payloadType")
  }

  private def objectStart(in: JsonReader): Unit = {
    if (!in.isNextToken('{')) in.decodeError("expected '{'")
  }

  /**
   * Fails on a duplicate key, play-json and jsoniter might disagree on which one wins.
   */
  private def markSeen(in: JsonReader, seen: Int, bit: Int): Int = {
    if ((seen & bit) != 0) in.decodeError("duplicate key")
    seen | bit
  }

  // `null` stands for a missing field, `readString` fails on JSON `null`
  private def originOf(value: String): Option[Origin] = if (value eq null) None else Some(Origin(value))

  private def versionOf(value: String): Option[Version] = if (value eq null) None else Some(Version(value))
}
