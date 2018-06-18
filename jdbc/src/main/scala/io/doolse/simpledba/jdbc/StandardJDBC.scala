package io.doolse.simpledba.jdbc

import java.sql.JDBCType.{SQLXML => _, _}
import java.sql._

import io.doolse.simpledba.jdbc.JDBCPreparedQuery.brackets


object StandardJDBC {
  def stdDropTableSQL(t: JDBCTableDefinition)(implicit mc: JDBCConfig) =
    s"DROP TABLE ${mc.escapeTableName(t.name)} IF EXISTS"

  def stdCreateTableSQL(t: JDBCTableDefinition, sqlTypeToString: (JDBCColumnBinding, Boolean) => String)(implicit mc: JDBCConfig): String = {
    val colStrings = t.columns.map { cb =>
      s"${mc.escapeColumnName(cb.name)} ${sqlTypeToString(cb, t.primaryKey.contains(cb.name))}${if (!cb.nullable) " NOT NULL" else ""}"
    }
    val withPK = colStrings :+ s"PRIMARY KEY${brackets(t.primaryKey.map(mc.escapeColumnName))}"
    s"CREATE TABLE ${mc.escapeTableName(t.name)} ${brackets(withPK)}"
  }

  def stdTruncateTable(t: JDBCTableDefinition)(implicit mc: JDBCConfig) : String =
    s"TRUNCATE TABLE ${mc.escapeTableName(t.name)}"

  val stdSQLTypeNames : Function[SQLType, String] = {
    case INTEGER => "INTEGER"
    case BIGINT => "BIGINT"
    case BOOLEAN => "BOOLEAN"
    case SMALLINT => "SMALLINT"
    case FLOAT => "FLOAT"
    case DOUBLE => "DOUBLE"
    case TIMESTAMP => "TIMESTAMP"
    case NVARCHAR => "NVARCHAR"
  }

  val DefaultReserved = Set("user", "begin", "end", "order", "by", "select", "from")

  val defaultEscapeReserved = escapeReserved(DefaultReserved) _
  val defaultParam = (_: JDBCColumnBinding) => "?"


  def escapeReserved(rw: Set[String])(s: String): String = {
    val lc = s.toLowerCase
    if (rw.contains(lc) || lc != s) '"' + s + '"' else s
  }


}
