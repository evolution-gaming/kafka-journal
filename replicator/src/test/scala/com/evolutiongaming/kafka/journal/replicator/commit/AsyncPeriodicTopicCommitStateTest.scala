package com.evolutiongaming.kafka.journal.replicator.commit

import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.replicator.SKafkaTestUtils.*
import com.evolutiongaming.kafka.journal.replicator.commit.AsyncPeriodicTopicCommit.*
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class AsyncPeriodicTopicCommitStateTest extends AnyFreeSpec with Matchers {

  "PartitionState" - {

    "has nothing to commit" - {
      "on start" in {
        pState().needToCommit shouldEqual none
      }
      "if nothing marked" in {
        pState(committed = 1.some).needToCommit shouldEqual none
      }
      "if marked offset is not higher than committed" in {
        pState(
          marked = 1.some,
          committed = 1.some,
        ).needToCommit shouldEqual none
      }
    }

    "has something to commit" - {
      "if there is a marked offset but nothing committed yet" in {
        pState(marked = 1.some).needToCommit shouldEqual offset(1).some
      }
      "if marked offset is higher than committed" in {
        pState(
          marked = 2.some,
          committed = 1.some,
        ).needToCommit shouldEqual offset(2).some
      }
    }

    "when updating marked offset" - {
      "updates if new value is higher" in {
        pState(
          marked = 2.some,
          committed = 1.some,
        ).updateMarkedIfNeeded(offset(3)) shouldEqual
          pState(
            marked = 3.some,
            committed = 1.some,
          )
      }

      "ignore update if the new value is not higher" in {
        pState(
          marked = 2.some,
          committed = 1.some,
        ).updateMarkedIfNeeded(offset(1)) shouldEqual
          pState(
            marked = 2.some,
            committed = 1.some,
          )
      }
    }

    "when updating committed offset" - {
      "updates if new value is higher" in {
        pState(
          marked = 1.some,
          committed = 1.some,
        ).updateCommittedIfNeeded(offset(2)) shouldEqual
          pState(
            marked = 1.some,
            committed = 2.some,
          )
      }

      "ignore update if the new value is not higher" in {
        pState(
          marked = 1.some,
          committed = 2.some,
        ).updateCommittedIfNeeded(offset(1)) shouldEqual
          pState(
            marked = 1.some,
            committed = 2.some,
          )
      }
    }
  }

  "State" - {
    "has something to commit when some partitions have uncommitted progress" in {
      var state = State()
      state = state.addAssignedPartitions(Vector(p(0), p(1), p(2)))
      state = state.updateCommittedOffsetsForAllAssigned(
        Vector(
          p(0) -> offset(1),
          p(1) -> offset(11),
          p(2) -> offset(21),
        ),
      )
      state = state.updateMarkedOffsetsForAllAssigned(
        Vector(
          p(0) -> offset(2),
          p(1) -> offset(11),
          p(2) -> offset(22),
        ),
      )

      state.partitionOffsetsToCommit shouldEqual Map(
        p(0) -> offset(2),
        p(2) -> offset(22),
      )
    }

    "removes revoked/lost partitions from commit" in {
      var state = State()
      state = state.addAssignedPartitions(Vector(p(0), p(1), p(2)))
      state = state.updateCommittedOffsetsForAllAssigned(
        Vector(
          p(0) -> offset(1),
          p(1) -> offset(11),
          p(2) -> offset(21),
        ),
      )
      state = state.updateMarkedOffsetsForAllAssigned(
        Vector(
          p(0) -> offset(2),
          p(1) -> offset(12),
          p(2) -> offset(22),
        ),
      )
      state.partitionOffsetsToCommit shouldEqual Map(
        p(0) -> offset(2),
        p(1) -> offset(12),
        p(2) -> offset(22),
      )

      state = state.removeAssignedPartitions(Vector(p(0)))

      state.partitionOffsetsToCommit shouldEqual Map(
        p(1) -> offset(12),
        p(2) -> offset(22),
      )
    }

    "ignores updates on non-assigned partitions" - {
      "when marked offsets updated" in {
        val state = State().addAssignedPartitions(Vector(p(0), p(2)))

        state.updateMarkedOffsetsForAllAssigned(
          Vector(
            p(0) -> offset(1),
            p(1) -> offset(2),
            p(2) -> offset(3),
          ),
        ) shouldEqual State(
          Map(
            p(0) -> pState(marked = 1.some),
            p(2) -> pState(marked = 3.some),
          ),
        )
      }

      "when committed offsets updated" in {
        val state = State().addAssignedPartitions(Vector(p(0), p(2)))

        state.updateCommittedOffsetsForAllAssigned(
          Vector(
            p(0) -> offset(1),
            p(1) -> offset(2),
            p(2) -> offset(3),
          ),
        ) shouldEqual State(
          Map(
            p(0) -> pState(committed = 1.some),
            p(2) -> pState(committed = 3.some),
          ),
        )
      }
    }
  }

  private def pState(marked: Option[Int] = None, committed: Option[Int] = None): PartitionState =
    PartitionState(
      markedOffsetOpt = marked.map(offset),
      committedOffsetOpt = committed.map(offset),
    )
}
