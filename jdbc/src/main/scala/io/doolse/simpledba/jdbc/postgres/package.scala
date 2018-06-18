package io.doolse.simpledba.jdbc

import java.sql.JDBCType.{DOUBLE, FLOAT, LONGNVARCHAR}
import java.sql.SQLType
import java.util.UUID

import fs2.Stream
import io.doolse.simpledba.{Iso, WriteOp}
import io.doolse.simpledba.jdbc.StandardJDBC._

package object postgres {
  case class PostgresColumn[AA](wrapped: StdJDBCColumn[AA]) extends WrappedColumn[AA, PostgresColumn]
  {
    override def mapped[B] = PostgresColumn.apply[B]
  }

  case object JSONBType extends SQLType {
    def getName = "JSONB"

    def getVendorTypeNumber = 1

    def getVendor = "io.doolse.simpledba.postgres"
  }

  object PostgresColumn
  {
    implicit def stdCol[A](implicit std: StdJDBCColumn[A]) = PostgresColumn(std)

    implicit def isoCol[A, B](implicit iso: Iso[B, A], pgCol: PostgresColumn[A]): PostgresColumn[B]
    = pgCol.isoMap(iso)

    implicit def uuidCol = PostgresColumn[UUID](StdJDBCColumn.uuidCol)

    def jsonbColumn = PostgresColumn(StdJDBCColumn.stringCol.copy(cast = c => s"$c::JSONB"))
  }

  def pgsqlTypeNames(cb: JDBCColumnBinding, key: Boolean) :  String = cb.sqlType match {
    case FLOAT => "REAL"
    case DOUBLE => "DOUBLE PRECISION"
    case LONGNVARCHAR => "TEXT"
    case UuidSQLType => "UUID"
    case o => stdSQLTypeNames(o)
  }

  def createPostgresParam(c: JDBCColumnBinding): String = {
    c.sqlType match {
      case JSONBType => "?::JSONB"
      case _ => "?"
    }
  }

  val postgresConfig = JDBCSQLConfig[PostgresColumn](defaultEscapeReserved, defaultEscapeReserved,
    createPostgresParam)

  def dropTable(t: JDBCTableDefinition)(implicit config: JDBCConfig): Stream[JDBCIO, WriteOp] =
    rawSQL(s"DROP TABLE IF EXISTS ${config.escapeTableName(t.name)} CASCADE")

  def createTable(t: JDBCTableDefinition)(implicit config: JDBCConfig): Stream[JDBCIO, WriteOp] =
    rawSQL(stdCreateTableSQL(t, pgsqlTypeNames))
}
