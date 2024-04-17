package com.evolutiongaming.kafka.journal

import com.evolutiongaming.scassandra.EncodeByName
import com.evolutiongaming.scassandra.DecodeByName
import com.evolutiongaming.scassandra.EncodeByIdx
import com.evolutiongaming.scassandra.DecodeByIdx
import com.evolutiongaming.scassandra.EncodeRow
import com.evolutiongaming.scassandra.DecodeRow

sealed abstract class SnapshotStatus {
  def name: String
}

object SnapshotStatus {

  val values: List[SnapshotStatus] = List(Active, Deleted)

  def fromStringUnsafe(status: String): SnapshotStatus =
    values.find(_.name == status).getOrElse(Active)

  case object Active extends SnapshotStatus { def name = "active" }
  case object Deleted extends SnapshotStatus { def name = "deleted" }

  implicit val encodeByNameSnapshotStatus: EncodeByName[SnapshotStatus] =
    EncodeByName[String].contramap(_.name)
  implicit val decodeByNameSnapshotStatus: DecodeByName[SnapshotStatus] =
    DecodeByName[String].map(fromStringUnsafe)

  implicit val encodeByIdxSnapshotStatus: EncodeByIdx[SnapshotStatus] =
    EncodeByIdx[String].contramap(_.name)
  implicit val decodeByIdxSnapshotStatus: DecodeByIdx[SnapshotStatus] =
    DecodeByIdx[String].map(fromStringUnsafe)

  implicit val encodeRowSeqNr: EncodeRow[SnapshotStatus] =
    EncodeRow[SnapshotStatus]("status")
  implicit val decodeRowSeqNr: DecodeRow[SnapshotStatus] =
    DecodeRow[SnapshotStatus]("status")
}
