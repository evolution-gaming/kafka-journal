package com.evolutiongaming.kafka.journal

import org.scalatest.funsuite.AnyFunSuite

class BufferNrSpec extends AnyFunSuite {

  type F[T] = Either[String, T]

  test("create BufferNr out of an Int") {
    BufferNr.of[F](17) match {
      case Left(message) => fail(message)
      case Right(bufferNr) => assert(bufferNr.value == 17)
    }
  }

  test("fail to create negative BufferNr") {
    BufferNr.of[F](-1) match {
      case Left(message) => assert(message == "invalid BufferNr of -1, it must be greater or equal to 0")
      case Right(bufferNr) => fail(s"exception was not thrown, but got $bufferNr instead")
    }
  }

  test("reuse BufferNr.min instance") {
    BufferNr.of[F](0) match {
      case Left(message) => fail(message)
      case Right(bufferNr) => assert(bufferNr == BufferNr.min)
    }
  }

  test("reuse BufferNr.max instance") {
    BufferNr.of[F](Int.MaxValue) match {
      case Left(message) => fail(message)
      case Right(bufferNr) => assert(bufferNr == BufferNr.max)
    }
  }

  test("use listOf") {
    assert(BufferNr.listOf(3).map(_.value) == List(0, 1, 2))
  }

}
