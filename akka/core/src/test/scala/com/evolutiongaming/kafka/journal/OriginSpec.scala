package com.evolutiongaming.kafka.journal

import cats.effect.IO
import com.evolutiongaming.kafka.journal.IOSuite.*
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

class OriginSpec extends AsyncFunSuite with Matchers {

  test("hostName") {
    val result = for {
      hostName <- Origin.hostName[IO]
      result = hostName.isDefined shouldEqual true
    } yield result
    result.run()
  }
}
