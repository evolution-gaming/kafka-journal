package akka.persistence.kafka.journal

import akka.persistence.{AtomicWrite, PersistentRepr}
import org.scalatest.{FunSuite, Matchers}

class BatchingSpec extends FunSuite with Matchers {

  private val atomicWrite = AtomicWrite(List(PersistentRepr(None, persistenceId = "persistenceId")))

  test("disabled") {
    val batching = Batching.disabled[cats.Id]
    batching(List(atomicWrite, atomicWrite)) shouldEqual List(List(atomicWrite), List(atomicWrite))
  }

  test("all") {
    val batching = Batching.all[cats.Id]
    batching(List(atomicWrite, atomicWrite)) shouldEqual List(List(atomicWrite, atomicWrite))
  }

  test("byNumberOfEvents 1") {
    val batching = Batching.byNumberOfEvents[cats.Id](1)
    batching(List(atomicWrite, atomicWrite)) shouldEqual List(List(atomicWrite), List(atomicWrite))
  }

  test("byNumberOfEvents 2") {
    val batching = Batching.byNumberOfEvents[cats.Id](2)
    batching(List(atomicWrite, atomicWrite)) shouldEqual List(List(atomicWrite, atomicWrite))
  }
}
