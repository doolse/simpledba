package io.doolse.simpledba.jdbc

import java.sql.{PreparedStatement, ResultSet, SQLType}
import java.sql.JDBCType._
import java.util.UUID

import io.doolse.simpledba.jdbc.JDBCColumn.{ArraySQLType, SizedSQLType, UuidSQLType}

/**
  * Created by jolz on 12/03/17.
  */

case class SpecialColumnType(byName: (ResultSet, String) => Option[AnyRef], byIndex: (ResultSet, Int) => Option[AnyRef],
                             bind: (PreparedStatement, Int, AnyRef) => Unit, bindingObject: AnyRef => AnyRef)

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
      (rs,i) => Option(rs.getString(i)).map(UUID.fromString),
      (ps, i, v) => ps.setString(i, v.toString),
      _.toString()
    )
  }

  val postgresSpecialCols : PartialFunction[SQLType, SpecialColumnType] = {
    case UuidSQLType => SpecialColumnType((rs,n) => Option(rs.getObject(n)),
      (rs, i) => Option(rs.getObject(i)),
      (ps, i, v) => ps.setObject(i, v),
      identity
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
    case NVARCHAR => "NVARCHAR"
    case SQLXML => "XML"
  }

  val hsqlTypeNames : SQLType => String = ({
    case UuidSQLType => "UUID"
    case LONGNVARCHAR => "LONGVARCHAR"
    case SizedSQLType(sub, size) => hsqlTypeNames(sub) + s"($size)"
  } : PartialFunction[SQLType, String]) orElse stdSQLTypeNames

  val pgsqlTypeNames : SQLType => String = ({
    case UuidSQLType => "UUID"
    case ArraySQLType(elem) => s"${pgsqlTypeNames(elem)} ARRAY"
    case FLOAT => "REAL"
    case DOUBLE => "DOUBLE PRECISION"
    case SizedSQLType(NVARCHAR, _) => "TEXT"
    case LONGNVARCHAR => "TEXT"
    case SizedSQLType(sub, size) => pgsqlTypeNames(sub) + s"($size)"
  } : PartialFunction[SQLType, String]) orElse stdSQLTypeNames

  val sqlServerTypeNames : SQLType => String = ({
    case UuidSQLType => "UNIQUEIDENTIFIER"
    case LONGNVARCHAR => "NVARCHAR(MAX)"
    case TIMESTAMP => "DATETIME"
    case BOOLEAN => "BIT"
    case SizedSQLType(sub, size) => sqlServerTypeNames(sub) + s"($size)"
  } : PartialFunction[SQLType, String]) orElse stdSQLTypeNames

  val oracleTypeNames : SQLType => String = ({
    case UuidSQLType => "CHAR(37)"
    case BIGINT => "NUMBER(19)"
    case NVARCHAR => "NVARCHAR2"
    case LONGNVARCHAR => "NVARCHAR2(2000)"
    case BOOLEAN => "NUMBER(1,0)"
    case SQLXML => "XMLTYPE"
    case SizedSQLType(sub, size) => oracleTypeNames(sub) + s"($size)"
  } : PartialFunction[SQLType, String]) orElse stdSQLTypeNames

  val DefaultReserved = Set("user", "begin", "end")

  val defaultEscapeReserved = escapeReserved(DefaultReserved) _
  val hsqldbConfig = JDBCSQLConfig(defaultEscapeReserved, defaultEscapeReserved, hsqlTypeNames, defaultDropTableSQL, stdSpecialCols)
  val postgresConfig = JDBCSQLConfig(defaultEscapeReserved, defaultEscapeReserved, pgsqlTypeNames,
    t => s"DROP TABLE IF EXISTS $t CASCADE", postgresSpecialCols)
  val sqlServerConfig = JDBCSQLConfig(defaultEscapeReserved, defaultEscapeReserved, sqlServerTypeNames,
    t => s"DROP TABLE IF EXISTS $t", stdSpecialCols)

  val oracleReserved = DefaultReserved ++ Set("session")
  val oracleEscapeReserved = escapeReserved(oracleReserved) _
  val oracleConfig = JDBCSQLConfig(oracleEscapeReserved, oracleEscapeReserved, oracleTypeNames,
    t => s"BEGIN EXECUTE IMMEDIATE 'DROP TABLE $t'; EXCEPTION WHEN OTHERS THEN NULL; END;", stdSpecialCols)

  def escapeReserved(rw: Set[String])(s: String): String = {
    val lc = s.toLowerCase
    if (rw.contains(lc) || lc != s) '"' + s + '"' else s
  }

}