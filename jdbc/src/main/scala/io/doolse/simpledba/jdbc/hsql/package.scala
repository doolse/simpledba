package io.doolse.simpledba.jdbc

import java.sql.JDBCType.LONGNVARCHAR
import java.sql.SQLType
import java.util.UUID

import fs2.Stream
import io.doolse.simpledba.WriteOp
import io.doolse.simpledba.jdbc.StandardJDBC._

package object hsql {
  case class HSQLColumn[AA](wrapped: StdJDBCColumn[AA]) extends WrappedColumn[AA, HSQLColumn]
  {
    def mapped[B] = HSQLColumn.apply[B]
  }

  object HSQLColumn
  {
    implicit def stdCol[A](implicit std: StdJDBCColumn[A]) = HSQLColumn(std)

    implicit def uuidCol = HSQLColumn[UUID](StdJDBCColumn.uuidCol)
  }

  def hsqlTypeNames(c: JDBCColumnBinding, key: Boolean) : String = c.sqlType match {
    case UuidSQLType => "UUID"
    case LONGNVARCHAR => "LONGVARCHAR"
    case o => stdSQLTypeNames(o)
  }

  val hsqldbConfig = JDBCSQLConfig[HSQLColumn](defaultEscapeReserved, defaultEscapeReserved, defaultParam)

  def dropTable(t: JDBCTableDefinition)(implicit config: JDBCConfig): Stream[JDBCIO, WriteOp] =
    rawSQL(stdDropTableSQL(t))

  def createTable(t: JDBCTableDefinition)(implicit config: JDBCConfig): Stream[JDBCIO, WriteOp] =
    rawSQL(stdCreateTableSQL(t, hsqlTypeNames))

}
