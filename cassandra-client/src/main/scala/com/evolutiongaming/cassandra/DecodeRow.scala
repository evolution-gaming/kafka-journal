package com.evolutiongaming.cassandra

import com.datastax.driver.core.Row


trait DecodeRow[A] extends { self =>

  def apply(row: Row): A

  final def map[B](f: A => B): DecodeRow[B] = new DecodeRow[B] {
    def apply(row: Row) = f(self(row))
  }
}

object DecodeRow {

  def apply[A](implicit decode: DecodeRow[A]): DecodeRow[A] = decode

  def apply[A](name: String)(implicit decode: Decode[A]): DecodeRow[A] = new DecodeRow[A] {
    def apply(row: Row) = decode(row, name)
  }
}
