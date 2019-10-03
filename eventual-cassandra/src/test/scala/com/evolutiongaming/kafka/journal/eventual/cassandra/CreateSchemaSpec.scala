package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Id
import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.scassandra.TableName
import org.scalatest.{FunSuite, Matchers}

class CreateSchemaSpec extends FunSuite with Matchers { self =>

  test("create keyspace and tables") {
    val config = SchemaConfig.default
    val createSchema = CreateSchema[StateT](config, createKeyspace, createTables)
    val initial = State.empty.copy(createTables = true)
    val (state, (schema, fresh)) = createSchema.run(initial)
    state shouldEqual initial.copy(actions = List(Action.CreateTables, Action.CreateKeyspace))
    fresh shouldEqual true
    schema shouldEqual self.schema
  }

  test("not create keyspace and tables") {
    val config = SchemaConfig.default.copy(autoCreate = false)
    val createSchema = CreateSchema[StateT](config, createKeyspace, createTables)
    val initial = State.empty.copy(createTables = true)
    val (state, (schema, fresh)) = createSchema.run(initial)
    state shouldEqual initial.copy(actions = List(Action.CreateKeyspace))
    fresh shouldEqual false
    schema shouldEqual self.schema
  }


  private val schema = Schema(
    journal = TableName(keyspace = "journal", table = "journal"),
    metadata = TableName(keyspace = "journal", table = "metadata"),
    pointer = TableName(keyspace = "journal", table = "pointer"),
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
    def apply(config: SchemaConfig.Keyspace) = {
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
