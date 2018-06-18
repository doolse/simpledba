package io.doolse.simpledba.jdbc

import java.sql.JDBCType.{BOOLEAN, LONGNVARCHAR, TIMESTAMP}
import java.sql.SQLType

import fs2.Stream
import io.doolse.simpledba.WriteOp
import io.doolse.simpledba.jdbc.StandardJDBC._

package object sqlserver {

  case class SQLServerColumn[AA](wrapped: StdJDBCColumn[AA]) extends WrappedColumn[AA, SQLServerColumn]
  {
    def mapped[B] = SQLServerColumn.apply[B]
  }

  object SQLServerColumn
  {
    implicit def stdCol[A](implicit std: StdJDBCColumn[A]) = SQLServerColumn(std)
  }

  def sqlServerTypeNames(stringKeySize: Int)(b: JDBCColumnBinding, key: Boolean): String = (b.sqlType, key) match {
    case (LONGNVARCHAR, true) => s"NVARCHAR($stringKeySize)"
    case (LONGNVARCHAR, false) => s"NVARCHAR(MAX)"
    case (TIMESTAMP, _) => "DATETIME"
    case (BOOLEAN, _) => "BIT"
    case (o, _) => stdSQLTypeNames(o)
  }

  def sqlServerConfig = JDBCSQLConfig[SQLServerColumn](defaultEscapeReserved, defaultEscapeReserved, defaultParam)

  def dropTable(t: JDBCTableDefinition)(implicit config: JDBCConfig): Stream[JDBCIO, WriteOp] =
    rawSQL(s"DROP TABLE IF EXISTS ${config.escapeTableName(t.name)}")

  def createTable(t: JDBCTableDefinition, types: (JDBCColumnBinding, Boolean) => String
                 = sqlServerTypeNames(256))(implicit config: JDBCConfig): Stream[JDBCIO, WriteOp] =
    rawSQL(stdCreateTableSQL(t, types))

}
