package com.evolutiongaming.kafka.journal.snapshot.cassandra

import cats.Id
import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CreateKeyspace, CreateTables, KeyspaceConfig}
import com.evolutiongaming.kafka.journal.snapshot.cassandra.{CreateSnapshotSchema, SnapshotSchemaConfig}
import com.evolutiongaming.scassandra.TableName
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CreateSchemaSpec extends AnyFunSuite with Matchers { self =>

  test("create keyspace and tables") {
    val config = SnapshotSchemaConfig.default
    val createSchema = CreateSnapshotSchema[StateT](config, createKeyspace, createTables)
    val initial = State.empty.copy(createTables = true)
    val (state, (schema, fresh)) = createSchema.run(initial)
    state shouldEqual initial.copy(actions = List(Action.CreateTables, Action.CreateKeyspace))
    fresh shouldEqual true
    schema shouldEqual self.schema
  }

  test("not create keyspace and tables") {
    val config = SnapshotSchemaConfig.default.copy(autoCreate = false)
    val createSchema = CreateSnapshotSchema[StateT](config, createKeyspace, createTables)
    val initial = State.empty.copy(createTables = true)
    val (state, (schema, fresh)) = createSchema.run(initial)
    state shouldEqual initial.copy(actions = List(Action.CreateKeyspace))
    fresh shouldEqual false
    schema shouldEqual self.schema
  }


  private val schema = SnapshotSchema(
    snapshot = TableName(keyspace = "journal", table = "snapshot_buffer"),
    setting = TableName(keyspace = "journal", table = "setting"))

  val createTables: CreateTables[StateT] = new CreateTables[StateT] {
    def apply(keyspace: String, tables: Nel[CreateTables.Table]) = {
      StateT { state =>
        val state1 = state.add(Action.CreateTables)
        (state1, state.createTables)
      }
    }
  }

  val createKeyspace: CreateKeyspace[StateT] = new CreateKeyspace[StateT] {
    def apply(config: KeyspaceConfig) = {
      StateT { state =>
        val state1 = state.add(Action.CreateKeyspace)
        (state1, ())
      }
    }
  }


  case class State(createTables: Boolean, actions: List[Action]) {

    def add(action: Action): State = copy(actions = action :: actions)
  }

  object State {
    val empty: State = State(createTables = false, actions = Nil)
  }


  type StateT[A] = cats.data.StateT[Id, State, A]

  object StateT {
    def apply[A](f: State => (State, A)): StateT[A] = cats.data.StateT[Id, State, A](f)
  }


  sealed trait Action extends Product

  object Action {
    case object CreateTables extends Action
    case object CreateKeyspace extends Action
  }
}
