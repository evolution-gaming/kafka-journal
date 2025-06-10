package akka.persistence.kafka.journal

import cats.arrow.FunctionK
import cats.effect.{IO, Outcome, Sync}
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.ActorSuite
import com.evolutiongaming.kafka.journal.IOSuite.*
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

class ActorSystemRefTest extends AsyncFunSuite with ActorSuite with Matchers {
  import ActorSystemRefTest.*

  test("Extension") {
    val result = for {
      ref <- Sync[IO].delay { Extension(actorSystem) }
      ref <- ref.fromFuture[IO].mapK(FunctionK.id).pure[IO]
      a <- ref.get.start
      _ <- ref.set(0)
      a <- a.join
      _ = a shouldEqual Outcome.Succeeded(IO.pure(0))
      a <- ref.get
      _ = a shouldEqual 0
      a <- ref.set(0).attempt
      _ = a.isLeft shouldEqual true
    } yield {}
    result.run()
  }
}

object ActorSystemRefTest {
  object Extension extends ActorSystemRef.ExtensionId[Int]
}
