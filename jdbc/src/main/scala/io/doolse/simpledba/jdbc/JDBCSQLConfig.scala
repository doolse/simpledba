package io.doolse.simpledba.jdbc

import java.sql.{PreparedStatement, ResultSet, SQLType}
import java.sql.JDBCType._
import java.util.UUID

import io.doolse.simpledba.jdbc.JDBCColumn.UuidSQLType

/**
  * Created by jolz on 12/03/17.
  */

case class SpecialColumnType(byName: (ResultSet, String) => Option[AnyRef], bind: (PreparedStatement, Int, AnyRef) => Unit)

case class JDBCSQLConfig(escapeTableName: String => String,
                         escapeColumnName: String => String,
                         sqlTypeToString: SQLType => String,
                         dropTable: String => String,
                         specialType: SQLType => SpecialColumnType)
{
  def dropTableSQL(dt: JDBCDropTable) : String = dropTable(escapeTableName(dt.name))
}

object JDBCSQLConfig {

  def defaultDropTableSQL(t: String) = s"DROP TABLE $t IF EXISTS"

  val stdSpecialCols : PartialFunction[SQLType, SpecialColumnType] = {
    case UuidSQLType => SpecialColumnType((rs,n) => Option(rs.getString(n)).map(UUID.fromString),
      (ps, i, v) => ps.setString(i, v.toString)
    )
  }

  val postgresSpecialCols : PartialFunction[SQLType, SpecialColumnType] = {
    case UuidSQLType => SpecialColumnType((rs,n) => Option(rs.getObject(n)),
      (ps, i, v) => ps.setObject(i, v)
    )
  }

  val stdSQLTypeNames : PartialFunction[SQLType, String] = {
    case INTEGER => "INTEGER"
    case BIGINT => "BIGINT"
    case BOOLEAN => "BOOLEAN"
    case SMALLINT => "SMALLINT"
    case FLOAT => "FLOAT"
    case DOUBLE => "DOUBLE"
    case TIMESTAMP => "TIMESTAMP"
  }

  val hsqlTypeNames : SQLType => String = ({
    case UuidSQLType => "UUID"
    case LONGNVARCHAR => "LONGVARCHAR"
  } : PartialFunction[SQLType, String]) orElse stdSQLTypeNames

  val pgsqlTypeNames : SQLType => String = ({
    case UuidSQLType => "UUID"
    case FLOAT => "REAL"
    case DOUBLE => "DOUBLE PRECISION"
    case LONGNVARCHAR => "TEXT"
  } : PartialFunction[SQLType, String]) orElse stdSQLTypeNames

  val DefaultReserved = Set("user")

  val defaultEscapeReserved = escapeReserved(DefaultReserved) _
  val hsqldbConfig = JDBCSQLConfig(defaultEscapeReserved, defaultEscapeReserved, hsqlTypeNames, defaultDropTableSQL, stdSpecialCols)
  val postgresConfig = JDBCSQLConfig(defaultEscapeReserved, defaultEscapeReserved, pgsqlTypeNames, t => s"DROP TABLE IF EXISTS $t", postgresSpecialCols)

  def escapeReserved(rw: Set[String])(s: String): String = {
    val lc = s.toLowerCase
    if (rw.contains(lc) || lc != s) '"' + s + '"' else s
  }

}
