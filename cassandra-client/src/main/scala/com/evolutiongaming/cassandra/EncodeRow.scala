package com.evolutiongaming.cassandra

import com.datastax.driver.core.BoundStatement


trait EncodeRow[-A] { self =>

  def apply(statement: BoundStatement, value: A): BoundStatement

  final def imap[B](f: B => A): EncodeRow[B] = new EncodeRow[B] {
    def apply(statement: BoundStatement, value: B) = self(statement, f(value))
  }
}

object EncodeRow {

  def apply[A](implicit encode: EncodeRow[A]): EncodeRow[A] = encode

  def apply[A](name: String)(implicit encode: Encode[A]): EncodeRow[A] = new EncodeRow[A] {
    def apply(statement: BoundStatement, value: A) = encode(statement, name, value)
  }
}