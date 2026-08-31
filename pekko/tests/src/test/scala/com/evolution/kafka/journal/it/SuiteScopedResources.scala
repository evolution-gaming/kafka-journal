package com.evolution.kafka.journal.it

import cats.effect.*
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentLinkedDeque
import scala.annotation.tailrec
import scala.util.control.NonFatal

/**
 * Allows integration tests to allocate Cats-Effect resources in suite constructors with automatic
 * destruction in `afterAll`.
 *
 * Intended usage example:
 * {{{
 *   class MyItSpec extends AnyFreeSpec with SuiteScopedResources {
 *     private val heavyComponent1: HeavyComponent = allocateSuiteScoped {
 *       // ResourceIO[HeavyComponent1] created here
 *       ...
 *     }
 *     private val heavyComponent2: HeavyComponent = allocateSuiteScoped {
 *       // ResourceIO[HeavyComponent2] created here
 *       // this block could use heavyComponent1 defined earlier
 *       ...
 *     }
 *
 *     // test code using heavyComponent1 and heavyComponent2 goes here
 *     ...
 *   }
 * }}}
 *
 * In case allocation fails for one of the resources, all previously created resources are
 * deallocated and the failure is propagated to the suite constructor.
 *
 * It is highly suggested not to assign allocateSuiteScoped result values to lazy val's, because on
 * Scala 2.13 it might result in deadlocks.
 */
private[it] trait SuiteScopedResources extends BeforeAndAfterAll { this: Suite =>
  import ItInstances.*
  import SuiteScopedResources.*

  private[this] val releaseIoDeque = new ConcurrentLinkedDeque[IO[Unit]]

  /**
   * Allocates a test-suite-scoped resource with automatic release.
   *
   * See [[SuiteScopedResources]] documentation for usage details.
   *
   * @param resource
   *   [[ResourceIO]] to allocate
   * @tparam T
   *   resource type
   */
  protected final def allocateSuiteScoped[T](resource: => ResourceIO[T]): T = {
    val (value, releaseIo) = try {
      resource.allocated.unsafeRunSync()
    } catch {
      case NonFatal(t) =>
        // not logging the exception stacktrace here because the exception is rethrown
        logger.error(s"Failed to allocate a suite-scoped resource, releasing already created")
        releaseAll()
        throw t
    }
    releaseIoDeque.addLast(releaseIo)
    value
  }

  @tailrec
  private def releaseAll(): Unit = {
    // releasing resources one-by-one in reverse addition order
    Option(releaseIoDeque.pollLast()) match {
      case None =>
        ()
      case Some(releaseIo) =>
        try {
          releaseIo.unsafeRunSync()
        } catch {
          case NonFatal(t) =>
            logger.warn("Failed to release suite-scoped resource", t)
        }
        releaseAll()
    }
  }

  override def afterAll(): Unit = {
    releaseAll()
    super.afterAll()
  }
}

private object SuiteScopedResources {
  private val logger = LoggerFactory.getLogger(classOf[SuiteScopedResources])
}
