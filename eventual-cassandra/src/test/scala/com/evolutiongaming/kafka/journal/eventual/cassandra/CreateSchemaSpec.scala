package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.data.{NonEmptyList as Nel, State}
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.cassandra.CreateTables.Fresh
import com.evolutiongaming.kafka.journal.cassandra.{CreateKeyspace, CreateTables, KeyspaceConfig}
import com.evolutiongaming.scassandra.TableName
import org.scalatest.funsuite.AnyFunSuite

class CreateSchemaSpec extends AnyFunSuite {

  type F[A] = State[Database, A]

  test("create keyspace and tables") {
    val config                      = SchemaConfig.default
    val createSchema                = CreateSchema.create[F](config, createKeyspace, createTables)
    val (database, (schema, fresh)) = createSchema.run(Database.empty).value
    assert(database.keyspaces == List("journal"))
    assert(
      database.tables.sorted == List(
        "journal.journal",
        "journal.metajournal",
        "journal.pointer2",
        "journal.setting",
      ),
    )
    assert(fresh)
    assert(schema == this.schema)
  }

  test("not create keyspace and tables") {
    val config = SchemaConfig
      .default
      .copy(
        autoCreate = false,
        keyspace   = KeyspaceConfig.default.copy(autoCreate = false),
      )
    val createSchema                = CreateSchema.create[F](config, createKeyspace, createTables)
    val (database, (schema, fresh)) = createSchema.run(Database.empty).value
    assert(database.keyspaces == Nil)
    assert(database.tables == Nil)
    assert(!fresh)
    assert(schema == this.schema)
  }

  test("create part of the tables") {
    val config = SchemaConfig
      .default
      .copy(
        keyspace = KeyspaceConfig.default.copy(autoCreate = false),
      )
    val initialState = Database
      .empty
      .copy(
        keyspaces = List("journal"),
        tables    = List("journal.setting"),
      )
    val createSchema                = CreateSchema.create[F](config, createKeyspace, createTables)
    val (database, (schema, fresh)) = createSchema.run(initialState).value
    assert(database.keyspaces == List("journal"))
    assert(
      database.tables.sorted == List(
        "journal.journal",
        "journal.metajournal",
        "journal.pointer2",
        "journal.setting",
      ),
    )
    assert(!fresh)
    assert(schema == this.schema)
  }

  private val schema = Schema(
    journal     = TableName(keyspace = "journal", table = "journal"),
    metaJournal = TableName(keyspace = "journal", table = "metajournal"),
    pointer2    = TableName(keyspace = "journal", table = "pointer2"),
    setting     = TableName(keyspace = "journal", table = "setting"),
  )

  val createTables: CreateTables[F] = new CreateTables[F] {
    def apply(keyspace: String, tables: Nel[CreateTables.Table]): F[Fresh] = {
      val results = tables.traverse { table =>
        assert(
          table
            .queries
            .head
            .contains(
              s"CREATE TABLE IF NOT EXISTS $keyspace.${table.name}",
            ),
        )
        Database.createTable(keyspace, table.name)
      }
      results.map(_.forall(_ == true))
    }
  }

  val createKeyspace: CreateKeyspace[F] = new CreateKeyspace[F] {
    def apply(config: KeyspaceConfig): F[Unit] =
      if (config.autoCreate) Database.createKeyspace(config.name)
      else ().pure[F]
  }

  case class Database(keyspaces: List[String], tables: List[String]) {

    def existsKeyspace(keyspace: String): Boolean =
      keyspaces.contains(keyspace)

    def createKeyspace(keyspace: String): Database =
      this.copy(keyspaces = keyspace :: keyspaces)

    def existsTable(keyspace: String, name: String): Boolean =
      tables.contains(s"$keyspace.$name")

    def createTable(keyspace: String, name: String): Database =
      this.copy(tables = s"$keyspace.$name" :: tables)

  }

  object Database {

    val empty: Database = Database(keyspaces = Nil, tables = Nil)

    def createKeyspace(keyspace: String): F[Unit] =
      State.modify(_.createKeyspace(keyspace))

    def createTable(keyspace: String, name: String): F[Boolean] =
      State { database =>
        if (!database.existsKeyspace(keyspace)) {
          fail(s"Keyspace '$keyspace' does not exist")
        }
        if (database.existsTable(keyspace, name)) {
          (database, false)
        } else {
          (database.createTable(keyspace, name), true)
        }
      }

  }

}
