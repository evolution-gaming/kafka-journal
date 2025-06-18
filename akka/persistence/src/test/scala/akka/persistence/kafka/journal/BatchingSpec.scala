package akka.persistence.kafka.journal

import akka.persistence.{AtomicWrite, PersistentRepr}
import cats.Id
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BatchingSpec extends AnyFunSuite with Matchers {

  private val atomicWrite = AtomicWrite(List(PersistentRepr(None, persistenceId = "persistenceId")))

  test("disabled") {
    val batching = Batching.disabled[Id]
    batching(List(atomicWrite, atomicWrite)) shouldEqual List(List(atomicWrite), List(atomicWrite))
  }

  test("all") {
    val batching = Batching.all[Id]
    batching(List(atomicWrite, atomicWrite)) shouldEqual List(List(atomicWrite, atomicWrite))
  }

  test("byNumberOfEvents 1") {
    val batching = Batching.byNumberOfEvents[Id](1)
    batching(List(atomicWrite, atomicWrite)) shouldEqual List(List(atomicWrite), List(atomicWrite))
  }

  test("byNumberOfEvents 2") {
    val batching = Batching.byNumberOfEvents[Id](2)
    batching(List(atomicWrite, atomicWrite)) shouldEqual List(List(atomicWrite, atomicWrite))
  }
}
