package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core._
import com.google.common.reflect.TypeToken

import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Date, UUID}
import java.{util => ju}

class RowStub extends Row {

  def notImplemented(): Nothing = sys.error("method is not implemented")

  override def isNull(i: Int): Boolean                                              = notImplemented()
  override def getBool(i: Int): Boolean                                             = notImplemented()
  override def getByte(i: Int): Byte                                                = notImplemented()
  override def getShort(i: Int): Short                                              = notImplemented()
  override def getInt(i: Int): Int                                                  = notImplemented()
  override def getLong(i: Int): Long                                                = notImplemented()
  override def getTimestamp(i: Int): Date                                           = notImplemented()
  override def getDate(i: Int): LocalDate                                           = notImplemented()
  override def getTime(i: Int): Long                                                = notImplemented()
  override def getFloat(i: Int): Float                                              = notImplemented()
  override def getDouble(i: Int): Double                                            = notImplemented()
  override def getBytesUnsafe(i: Int): ByteBuffer                                   = notImplemented()
  override def getBytes(i: Int): ByteBuffer                                         = notImplemented()
  override def getString(i: Int): String                                            = notImplemented()
  override def getVarint(i: Int): BigInteger                                        = notImplemented()
  override def getDecimal(i: Int): java.math.BigDecimal                             = notImplemented()
  override def getUUID(i: Int): UUID                                                = notImplemented()
  override def getInet(i: Int): InetAddress                                         = notImplemented()
  override def getList[T <: Object](i: Int, elementsClass: Class[T]): ju.List[T]    = notImplemented()
  override def getList[T <: Object](i: Int, elementsType: TypeToken[T]): ju.List[T] = notImplemented()
  override def getSet[T <: Object](i: Int, elementsClass: Class[T]): ju.Set[T]      = notImplemented()
  override def getSet[T <: Object](i: Int, elementsType: TypeToken[T]): ju.Set[T]   = notImplemented()
  override def getMap[K <: Object, V <: Object](i: Int, keysClass: Class[K], valuesClass: Class[V]): ju.Map[K, V] =
    notImplemented()
  override def getMap[K <: Object, V <: Object](i: Int, keysType: TypeToken[K], valuesType: TypeToken[V]): ju.Map[K, V] =
    notImplemented()
  override def getUDTValue(i: Int): UDTValue                                              = notImplemented()
  override def getTupleValue(i: Int): TupleValue                                          = notImplemented()
  override def getObject(i: Int): Object                                                  = notImplemented()
  override def get[T <: Object](i: Int, targetClass: Class[T]): T                         = notImplemented()
  override def get[T <: Object](i: Int, targetType: TypeToken[T]): T                      = notImplemented()
  override def get[T <: Object](i: Int, codec: TypeCodec[T]): T                           = notImplemented()
  override def isNull(name: String): Boolean                                              = notImplemented()
  override def getBool(name: String): Boolean                                             = notImplemented()
  override def getByte(name: String): Byte                                                = notImplemented()
  override def getShort(name: String): Short                                              = notImplemented()
  override def getInt(name: String): Int                                                  = notImplemented()
  override def getLong(name: String): Long                                                = notImplemented()
  override def getTimestamp(name: String): Date                                           = notImplemented()
  override def getDate(name: String): LocalDate                                           = notImplemented()
  override def getTime(name: String): Long                                                = notImplemented()
  override def getFloat(name: String): Float                                              = notImplemented()
  override def getDouble(name: String): Double                                            = notImplemented()
  override def getBytesUnsafe(name: String): ByteBuffer                                   = notImplemented()
  override def getBytes(name: String): ByteBuffer                                         = notImplemented()
  override def getString(name: String): String                                            = notImplemented()
  override def getVarint(name: String): BigInteger                                        = notImplemented()
  override def getDecimal(name: String): java.math.BigDecimal                             = notImplemented()
  override def getUUID(name: String): UUID                                                = notImplemented()
  override def getInet(name: String): InetAddress                                         = notImplemented()
  override def getList[T <: Object](name: String, elementsClass: Class[T]): ju.List[T]    = notImplemented()
  override def getList[T <: Object](name: String, elementsType: TypeToken[T]): ju.List[T] = notImplemented()
  override def getSet[T <: Object](name: String, elementsClass: Class[T]): ju.Set[T]      = notImplemented()
  override def getSet[T <: Object](name: String, elementsType: TypeToken[T]): ju.Set[T]   = notImplemented()
  override def getMap[K <: Object, V <: Object](name: String, keysClass: Class[K], valuesClass: Class[V]): ju.Map[K, V] =
    notImplemented()
  override def getMap[K <: Object, V <: Object](name: String, keysType: TypeToken[K], valuesType: TypeToken[V]): ju.Map[K, V] =
    notImplemented()
  override def getUDTValue(name: String): UDTValue                         = notImplemented()
  override def getTupleValue(name: String): TupleValue                     = notImplemented()
  override def getObject(name: String): Object                             = notImplemented()
  override def get[T <: Object](name: String, targetClass: Class[T]): T    = notImplemented()
  override def get[T <: Object](name: String, targetType: TypeToken[T]): T = notImplemented()
  override def get[T <: Object](name: String, codec: TypeCodec[T]): T      = notImplemented()
  override def getColumnDefinitions(): ColumnDefinitions                   = notImplemented()
  override def getToken(i: Int): Token                                     = notImplemented()
  override def getToken(name: String): Token                               = notImplemented()
  override def getPartitionKeyToken(): Token                               = notImplemented()

}
