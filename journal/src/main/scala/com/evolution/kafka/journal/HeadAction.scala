package com.evolution.kafka.journal

import com.evolutiongaming.skafka.Offset
import com.github.plokhotnyuk.jsoniter_scala.core.*

/**
 * Part of [[ActionHeader]] used by [[HeadCache]]: the kind of the action and its sequence numbers.
 *
 * [[HeadCache]] reads every record of a journal topic, but keeps only [[HeadInfo]] for each
 * journal. To build [[HeadInfo]] it does not need `origin`, `version`, `payloadType` or `metadata`
 * of [[ActionHeader]], so [[HeadAction.fromJson]] does not decode them.
 */
private[journal] sealed abstract class HeadAction extends Product

private[journal] object HeadAction {

  final case class Append(range: SeqRange) extends HeadAction

  final case class Delete(to: DeleteTo) extends HeadAction

  case object Purge extends HeadAction

  case object Mark extends HeadAction

  def apply(header: ActionHeader): HeadAction = {
    header match {
      case a: ActionHeader.Append => Append(a.range)
      case a: ActionHeader.Delete => Delete(a.to)
      case _: ActionHeader.Purge => Purge
      case _: ActionHeader.Mark => Mark
    }
  }

  implicit class HeadActionOps(val self: HeadAction) extends AnyVal {

    /**
     * Same as [[HeadInfo.HeadInfoOps#apply]] for the corresponding [[ActionHeader]].
     */
    def applyTo(headInfo: HeadInfo, offset: Offset): HeadInfo = {
      self match {
        case a: Append => headInfo.append(a.range, offset)
        case a: Delete => headInfo.delete(a.to)
        case Purge => headInfo.purge
        case Mark => headInfo.mark
      }
    }
  }

  /**
   * Decodes `journal.action` header JSON directly into [[HeadAction]]
   *
   * The decoder gives the same action as the full decoder ([[ActionHeader.formatOptActionHeader]]):
   * it expects a JSON object with exactly one action key, and checks the fields that the full
   * decoder checks. For everything else, e.g. unknown keys, duplicate keys or invalid values, it
   * returns `None`, and the caller must use the full decoder, which then either decodes the header
   * or reports the error.
   *
   * `metadata` of an append: it is skipped, not decoded, so the head cache
   * does not depend on its content.
   *
   * @return
   *   `None` if the caller must use the full decoder, `Some(None)` if the header has no action,
   *   `Some(Some(action))` otherwise.
   */
  def fromJson(bytes: Array[Byte]): Option[Option[HeadAction]] = {
    try {
      Some(readFromArray(bytes)(codec))
    } catch {
      case _: JsonReaderException => None
    }
  }

  private val codec: JsonValueCodec[Option[HeadAction]] = new JsonValueCodec[Option[HeadAction]] {

    def nullValue: Option[HeadAction] = None

    def encodeValue(x: Option[HeadAction], out: JsonWriter): Unit = {
      throw new UnsupportedOperationException("HeadAction is decoded only")
    }

    def decodeValue(in: JsonReader, default: Option[HeadAction]): Option[HeadAction] = {
      var action: HeadAction = null
      readObject(in) { len =>
        if (action ne null) in.decodeError("more than one action")
        action = {
          if (in.isCharBufEqualsTo(len, "append")) readAppend(in)
          else if (in.isCharBufEqualsTo(len, "delete")) readDelete(in)
          else if (in.isCharBufEqualsTo(len, "purge")) readPurge(in)
          else if (in.isCharBufEqualsTo(len, "mark")) readMark(in)
          else in.decodeError("unknown action")
        }
      }
      Option(action)
    }
  }

  /**
   * Reads a JSON object and calls `field` for each key, with the reader at the value. `field` must
   * read or skip the value.
   */
  private def readObject(in: JsonReader)(field: Int => Unit): Unit = {
    if (!in.isNextToken('{')) in.decodeError("expected '{'")
    if (!in.isNextToken('}')) {
      in.rollbackToken()
      while ({
        field(in.readKeyAsCharBuf())
        in.isNextToken(',')
      }) ()
      if (!in.isCurrentToken('}')) in.objectEndOrCommaError()
    }
  }

  private def readAppend(in: JsonReader): HeadAction = {
    var range: SeqRange = null
    var payloadType = false
    readObject(in) { len =>
      if (in.isCharBufEqualsTo(len, "range")) {
        if (range ne null) in.decodeError("duplicate range")
        range = readRange(in)
      } else if (in.isCharBufEqualsTo(len, "payloadType")) {
        if (payloadType) in.decodeError("duplicate payloadType")
        readPayloadType(in)
        payloadType = true
      } else if (isOriginOrVersion(in, len)) {
        readOptionalString(in)
      } else {
        in.skip()
      }
    }
    if ((range eq null) || !payloadType) in.decodeError("expected range and payloadType")
    Append(range)
  }

  private def readDelete(in: JsonReader): HeadAction = {
    var to = 0L
    readObject(in) { len =>
      if (in.isCharBufEqualsTo(len, "to")) {
        if (to != 0L) in.decodeError("duplicate to")
        to = readSeqNr(in)
      } else if (isOriginOrVersion(in, len)) {
        readOptionalString(in)
      } else {
        in.skip()
      }
    }
    if (to == 0L) in.decodeError("expected to")
    Delete(SeqNr.unsafe(to).toDeleteTo)
  }

  private def readPurge(in: JsonReader): HeadAction = {
    readObject(in) { len =>
      if (isOriginOrVersion(in, len)) readOptionalString(in) else in.skip()
    }
    Purge
  }

  private def readMark(in: JsonReader): HeadAction = {
    var id = false
    readObject(in) { len =>
      if (in.isCharBufEqualsTo(len, "id")) {
        if (id) in.decodeError("duplicate id")
        if (in.readString(null) eq null) in.decodeError("invalid id")
        id = true
      } else if (isOriginOrVersion(in, len)) {
        readOptionalString(in)
      } else {
        in.skip()
      }
    }
    if (!id) in.decodeError("expected id")
    Mark
  }

  private def readRange(in: JsonReader): SeqRange = {
    var from = 0L
    var to = 0L
    readObject(in) { len =>
      if (in.isCharBufEqualsTo(len, "from")) {
        if (from != 0L) in.decodeError("duplicate from")
        from = readSeqNr(in)
      } else if (in.isCharBufEqualsTo(len, "to")) {
        if (to != 0L) in.decodeError("duplicate to")
        to = readSeqNr(in)
      } else {
        in.skip()
      }
    }
    if (from == 0L || to == 0L || from > to) in.decodeError("invalid range")
    SeqRange(SeqNr.unsafe(from), SeqNr.unsafe(to))
  }

  /**
   * Same check as `Reads[SeqNr]`: an integer number not less than [[SeqNr.min]].
   */
  private def readSeqNr(in: JsonReader): Long = {
    val value = in.readLong()
    if (value < SeqNr.min.value) in.decodeError("invalid SeqNr")
    value
  }

  /**
   * Same check as `Reads[PayloadType.BinaryOrJson]`.
   */
  private def readPayloadType(in: JsonReader): Unit = {
    val value = in.readString(null)
    if (value != PayloadType.Binary.name && value != PayloadType.Json.name) in.decodeError("invalid payloadType")
  }

  private def isOriginOrVersion(in: JsonReader, len: Int): Boolean = {
    in.isCharBufEqualsTo(len, "origin") || in.isCharBufEqualsTo(len, "version")
  }

  /**
   * Same check as `Reads[Option[Origin]]` and `Reads[Option[Version]]` in a case class format: a
   * string or `null` (`readString` returns the default for `null`).
   */
  private def readOptionalString(in: JsonReader): Unit = {
    in.readString(null)
    ()
  }
}
