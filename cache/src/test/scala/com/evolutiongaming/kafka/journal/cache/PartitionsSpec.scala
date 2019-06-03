package com.evolutiongaming.kafka.journal.cache

import cats.Id
import org.scalatest.{Matchers, WordSpec}

class PartitionsSpec extends WordSpec with Matchers {

  "apply" should {

    val partitions = Partitions.of[Id, Int, String](3, _.toString, identity)

    "get" in {
      for {
        n <- 0 to 10
      } yield {
        partitions.get(n) shouldEqual (n % 3).toString
      }
    }

    "Partitions.values" in {
      partitions.values shouldEqual List("0", "1", "2")
    }
  }


  "const" should {

    val partitions = Partitions.const[Int, String]("0")

    "get" in {
      for {
        n <- 0 to 10
      } yield {
        partitions.get(n) shouldEqual "0"
      }
    }

    "Partitions.values" in {
      partitions.values shouldEqual List("0")
    }
  }
}
