package com.evolutiongaming.concurrent.async

import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

class AsyncSpec extends FunSuite with Matchers {

  private val timeout = 3.seconds

  test("map") {
    Async(1).map(_ + 1) shouldEqual Async(2)
    Async(1).map(_ => throw Error) shouldEqual Async.failed(Error)
    Async.failed[Int](Error).map(_ + 1) shouldEqual Async.failed(Error)
    Async.async(1).map(_ + 1).await(timeout) shouldEqual Async(2)
    Async.async(1).await(timeout).map(_ + 1) shouldEqual Async(2)
  }

  test("flatMap") {
    Async(1).flatMap(_ => Async(2)) shouldEqual Async(2)
    Async(1).flatMap(_ => Async.failed(Error)) shouldEqual Async.failed(Error)
    Async(1).flatMap(_ => throw Error) shouldEqual Async.failed(Error)
    Async(1).flatMap(_ => Async.async(2)).await(timeout) shouldEqual Async(2)


    Async.failed(Error).flatMap(_ => Async(2)) shouldEqual Async.failed(Error)
    Async.failed(Error).flatMap(_ => Async.failed(Error)) shouldEqual Async.failed(Error)
    Async.failed(Error).flatMap(_ => throw Error) shouldEqual Async.failed(Error)
    Async.failed(Error).flatMap(_ => Async.async(2)) shouldEqual Async.failed(Error)


    Async.async(1).flatMap(_ => Async(2)).await(timeout) shouldEqual Async(2)
    Async.async(1).flatMap(_ => Async.failed(Error)).await(timeout) shouldEqual Async.failed(Error)
    Async.async(1).flatMap(_ => throw Error).await(timeout) shouldEqual Async.failed(Error)
    Async.async(1).flatMap(_ => Async.async(2)).await(timeout) shouldEqual Async(2)
  }

  test("value") {
    Async(1).value shouldEqual Some(Success(1))
    Async.failed(Error).value shouldEqual Some(Failure(Error))
    Async.never[Int].value shouldEqual None
  }

  test("get") {
    Async(1).get(timeout) shouldEqual 1
    the[Error.type] thrownBy Async.failed(Error).get(timeout)
    Async.async(1).get(timeout) shouldEqual 1
  }

  private case object Error extends RuntimeException with NoStackTrace
}
