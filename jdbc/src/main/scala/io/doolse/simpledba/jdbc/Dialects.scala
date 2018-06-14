package io.doolse.simpledba.jdbc

import java.sql.JDBCType.{SQLXML => _, _}
import java.sql._
import java.util.UUID

import io.doolse.simpledba.Iso


case class HSQLColumn[AA](wrapped: StdJDBCColumn[AA]) extends WrappedColumn[AA, HSQLColumn]
{
  def mapped[B] = HSQLColumn.apply[B]
}

object HSQLColumn
{
  implicit def stdCol[A](implicit std: StdJDBCColumn[A]) = HSQLColumn(std)

  implicit def uuidCol = HSQLColumn[UUID](StdJDBCColumn.uuidCol)
}

object Dialects {
  def defaultDropTableSQL(t: String) = s"DROP TABLE $t IF EXISTS"

  val stdSQLTypeNames : PartialFunction[SQLType, String] = {
    case INTEGER => "INTEGER"
    case BIGINT => "BIGINT"
    case BOOLEAN => "BOOLEAN"
    case SMALLINT => "SMALLINT"
    case FLOAT => "FLOAT"
    case DOUBLE => "DOUBLE"
    case TIMESTAMP => "TIMESTAMP"
    case NVARCHAR => "NVARCHAR"
  }

  val hsqlTypeNames : SQLType => String = ({
    case UuidSQLType => "UUID"
    case LONGNVARCHAR => "LONGVARCHAR"
  } : PartialFunction[SQLType, String]) orElse stdSQLTypeNames

  val sqlServerTypeNames : SQLType => String = ({
    case LONGNVARCHAR => "NVARCHAR(MAX)"
    case TIMESTAMP => "DATETIME"
    case BOOLEAN => "BIT"
  } : PartialFunction[SQLType, String]) orElse stdSQLTypeNames

  val oracleTypeNames : SQLType => String = ({
    case BIGINT => "NUMBER(19)"
    case NVARCHAR => "NVARCHAR2"
    case LONGNVARCHAR => "NVARCHAR2(2000)"
    case BOOLEAN => "NUMBER(1,0)"
  } : PartialFunction[SQLType, String]) orElse stdSQLTypeNames

  val DefaultReserved = Set("user", "begin", "end", "order", "by", "select", "from")

  val defaultEscapeReserved = escapeReserved(DefaultReserved) _
  val defaultEscapeColumn = (escapeReserved(DefaultReserved) _).compose[JDBCColumnBinding](_.name)
  val defaultParam = (_: JDBCColumnBinding) => "?"
  val hsqldbConfig = JDBCSQLConfig[HSQLColumn](defaultEscapeReserved, defaultEscapeColumn, defaultParam, hsqlTypeNames, defaultDropTableSQL)

//  val sqlServerConfig = JDBCSQLConfig(defaultEscapeReserved, defaultEscapeReserved, sqlServerTypeNames,
//    t => s"DROP TABLE IF EXISTS $t", stdSpecialCols)
//
//  val oracleReserved = DefaultReserved ++ Set("session")
//  val oracleEscapeReserved = escapeReserved(oracleReserved) _
//  val oracleConfig = JDBCSQLConfig(oracleEscapeReserved, oracleEscapeReserved, oracleTypeNames,
//    t => s"BEGIN EXECUTE IMMEDIATE 'DROP TABLE $t'; EXCEPTION WHEN OTHERS THEN NULL; END;", stdSpecialCols)

  def escapeReserved(rw: Set[String])(s: String): String = {
    val lc = s.toLowerCase
    if (rw.contains(lc) || lc != s) '"' + s + '"' else s
  }


}
