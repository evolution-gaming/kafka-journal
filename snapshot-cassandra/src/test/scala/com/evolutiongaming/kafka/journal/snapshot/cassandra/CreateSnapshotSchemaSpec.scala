package com.evolutiongaming.kafka.journal.snapshot.cassandra

import cats.data.{NonEmptyList => Nel, State}
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.cassandra.{CreateKeyspace, CreateTables, KeyspaceConfig}
import com.evolutiongaming.scassandra.TableName
import org.scalatest.funsuite.AnyFunSuite

class CreateSchemaSpec extends AnyFunSuite {

  type F[A] = State[Database, A]

  test("create keyspace and tables") {
    val config = SnapshotSchemaConfig.default
    val createSchema = CreateSnapshotSchema[F](config, createKeyspace, createTables)
    val (database, (schema, fresh)) = createSchema.run(Database.empty).value
    assert(database.keyspaces == List("snapshot"))
    assert(
      database.tables.sorted == List(
        "snapshot.setting",
        "snapshot.snapshot_buffer"
      )
    )
    assert(fresh)
    assert(schema == this.schema)
  }

  test("not create keyspace and tables") {
    val config = SnapshotSchemaConfig.default.copy(
      autoCreate = false,
      keyspace = SnapshotSchemaConfig.default.keyspace.copy(autoCreate = false)
    )
    val createSchema = CreateSnapshotSchema[F](config, createKeyspace, createTables)
    val (database, (schema, fresh)) = createSchema.run(Database.empty).value
    assert(database.keyspaces == Nil)
    assert(database.tables == Nil)
    assert(!fresh)
    assert(schema == this.schema)
  }

  test("create part of the tables") {
    val config = SnapshotSchemaConfig.default.copy(
      keyspace = SnapshotSchemaConfig.default.keyspace.copy(autoCreate = false)
    )
    val initialState = Database.empty.copy(
      keyspaces = List("snapshot"),
      tables = List("snapshot.setting")
    )
    val createSchema = CreateSnapshotSchema[F](config, createKeyspace, createTables)
    val (database, (schema, fresh)) = createSchema.run(initialState).value
    assert(database.keyspaces == List("snapshot"))
    assert(
      database.tables.sorted == List(
        "snapshot.setting",
        "snapshot.snapshot_buffer"
      )
    )
    assert(!fresh)
    assert(schema == this.schema)
  }

  private val schema = SnapshotSchema(
    snapshot = TableName(keyspace = "snapshot", table = "snapshot_buffer"),
    setting = TableName(keyspace = "snapshot", table = "setting")
  )

  val createTables: CreateTables[F] = new CreateTables[F] {
    def apply(keyspace: String, tables: Nel[CreateTables.Table]) = {
      val results = tables.traverse { table =>
        assert(
          table.queries.head.contains(
            s"CREATE TABLE IF NOT EXISTS $keyspace.${table.name}"
          )
        )
        Database.createTable(keyspace, table.name)
      }
      results.map(_.forall(_ == true))
    }
  }

  val createKeyspace: CreateKeyspace[F] = new CreateKeyspace[F] {
    def apply(config: KeyspaceConfig) =
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
