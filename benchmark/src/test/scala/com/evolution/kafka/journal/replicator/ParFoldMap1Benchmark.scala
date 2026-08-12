package com.evolution.kafka.journal.replicator

import cats.data.{NonEmptyList, NonEmptyMap}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.evolution.kafka.journal.replicator.Poll.{chanceToFailOneIn, error}
import com.evolutiongaming.catshelper.ParallelHelper.*
import com.evolutiongaming.skafka.Partition
import org.openjdk.jmh.annotations.{Benchmark, Fork, Measurement, Scope, State, Warmup}
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit
import scala.collection.immutable.SortedMap
import scala.util.Random
import scala.util.control.NoStackTrace

/**
 * Benchmarks cancellation effect on hot-path in [[TopicReplicator]] and [[ReplicateRecords]]:
 *   - `poll` returns `NonEmptyMap[Partition, NonEmptyList[Record[Key, Value]]]`, usually batch
 *     contains up to 1000 records
 *   - use `parFoldMap1` to process all partitions in parallel
 *   - use `parFoldMap1` to process partition's records grouped by key
 *   - randomly processing of some key may fail, which triggers cancellation of parallel processing
 *
 * To run benchmarks:
 * {{{sbt benchmark/Jmh/run com.evolution.kafka.journal.replicator.ParFoldMap1Benchmark}}}
 *
 * Results on Apple M3 Max using Oracle JDK 17
 * Benchmark                                     Mode  Cnt     Score     Error  Units
 * ParFoldMap1Benchmark.originalPost11_0_0      thrpt    5  3419.803 ±  43.579  ops/s
 * ParFoldMap1Benchmark.originalPost11_0_0Plus  thrpt    5  3456.063 ±  42.177  ops/s
 * ParFoldMap1Benchmark.originalPre11_0_0       thrpt    5  3357.712 ± 155.907  ops/s
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
class ParFoldMap1Benchmark {

  @Benchmark
  def originalPre11_0_0(blackhole: Blackhole): Unit = {
    val topicReplicator = TopicReplicatorPre11_0_0(_)
    run(blackhole, topicReplicator)
  }

  @Benchmark
  def originalPost11_0_0(blackhole: Blackhole): Unit = {
    val topicReplicator = TopicReplicatorPost11_0_0(_)
    run(blackhole, topicReplicator)
  }

  @Benchmark
  def originalPost11_0_0Plus(blackhole: Blackhole): Unit = {
    val topicReplicator = TopicReplicatorPost11_0_0Plus(_)
    run(blackhole, topicReplicator)
  }

  def run(blackhole: Blackhole, topicReplicator: NonEmptyMap[Partition, NonEmptyList[Record]] => IO[Int]): Unit = {
    blackhole.consume(
      IO(Poll.generate)
        .flatMap {
          topicReplicator(_)
            .handleError { _ => 0 }
        }
        .unsafeRunSync(),
    )
  }
}

//object Test extends IOApp {
//
//  override def run(args: List[String]): IO[ExitCode] = {
//    val result = {
//      IO(Poll.generate).flatMap {
//        TopicReplicatorPost11_0_0(_)
//          .handleError { e => println(s"got error: $e"); 0 }
//          .map { total => println(s"processed: $total"); total }
//      }
//    }
//    result.replicateA(10).map(_.sum).map(sum => println(s"sum: $sum")).as(ExitCode.Success)
//  }
//
//}

case class Record(key: String, value: Int)

object Poll {

  val chanceToFailOneIn = 1_00
  val error: Throwable = new RuntimeException("ba-bam!") with NoStackTrace

  def generate: NonEmptyMap[Partition, NonEmptyList[Record]] = {
    val numberOfKeys = Random.nextInt(250) + 1
    val numberOfRecords = Random.nextInt(1000) + 1
    val numberOfPartitions = Random.nextInt(16) + 1

    val keys = (0 until numberOfKeys).map(i => s"key: $i")
    val records = (0 until numberOfRecords).map(Record(keys(Random.nextInt(numberOfKeys)), _))

    NonEmptyMap.fromMapUnsafe {
      SortedMap.from {
        records.groupBy(_.value % numberOfPartitions).map { case (p, r) =>
          Partition.unsafe(p) -> NonEmptyList.fromListUnsafe(r.toList)
        }
      }
    }
  }
}

object ReplicateRecords {
  def process(record: Record): IO[NonEmptyList[Int]] =
    if (Random.nextInt(chanceToFailOneIn) == 0) IO.raiseError(error)
    else IO(NonEmptyList.one(1))
}

// Pre11_0_0 - "clean" version
object TopicReplicatorPre11_0_0 {
  def apply(records: NonEmptyMap[Partition, NonEmptyList[Record]]): IO[Int] = {
    for {
      result <- records.parFoldMap1 {
        case (partition, records) =>
          records
            .groupBy { _.key }
            .parFoldMap1 {
              case (key, records) =>
                for {
                  result <- records.flatTraverse(ReplicateRecords.process)
                  size <- result.size.pure[IO]
                } yield size
            }
      }
    } yield result
  }
}

// Post11_0_0 - each processing returns `Either` and we `.flatMap { Concurrent[F].fromEither }`
object TopicReplicatorPost11_0_0 {

  def apply(records: NonEmptyMap[Partition, NonEmptyList[Record]]): IO[Int] = {
    for {
      result <- records.parFoldMap1 {
        case (partition, records) =>
          records
            .groupBy { _.key }
            .parFoldMap1 {
              case (key, records) =>
                val result = for {
                  result <- records.flatTraverse(ReplicateRecords.process)
                  size <- result.size.pure[IO]
                } yield size
                result.attempt
            }
            .flatMap { IO.fromEither }
      }
    } yield result
  }
}

// Post11_0_0Plus - each processing returns `Either` and we `.flatMap { Concurrent[F].fromEither }`
object TopicReplicatorPost11_0_0Plus {

  def apply(records: NonEmptyMap[Partition, NonEmptyList[Record]]): IO[Int] = {
    for {
      result <- records.parFoldMap1 {
        case (partition, records) =>
          val result = records
            .groupBy { _.key }
            .parFoldMap1 {
              case (key, records) =>
                val result = for {
                  result <- records.flatTraverse(ReplicateRecords.process)
                  size <- result.size.pure[IO]
                } yield size
                result.attempt
            }
            .flatMap { IO.fromEither }

          result.attempt
      }
        .flatMap { IO.fromEither }
    } yield result
  }
}
