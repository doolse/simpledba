package io.doolse.simpledba.jdbc

import java.sql.JDBCType.{SQLXML => _, _}
import java.sql._
import java.util.UUID

case class PostgresColumn[AA](col: StdJDBCColumn[AA]) extends JDBCColumn
{
  type A = AA

  override def sqlType: SQLType = col.sqlType

  override def nullable: Boolean = col.nullable

  override def getByIndex: (Int, ResultSet) => Option[AA] = col.getByIndex

  override def bind: (Int, AA, Connection, PreparedStatement) => Unit = col.bind
}

object PostgresColumn
{
  implicit def stdCol[A](implicit std: StdJDBCColumn[A]) = PostgresColumn(std)

  implicit def uuidCol = PostgresColumn[UUID](
    StdJDBCColumn(UuidSQLType, false,
      (i,rs) => rs.getObject(i).asInstanceOf[UUID], (i,v,_,ps) => ps.setObject(i, v)))
}

object PostgresMapper {
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
    case LONGNVARCHAR => "LONGVARCHAR"
  } : PartialFunction[SQLType, String]) orElse stdSQLTypeNames

  val pgsqlTypeNames : SQLType => String = ({
    case FLOAT => "REAL"
    case DOUBLE => "DOUBLE PRECISION"
    case LONGNVARCHAR => "TEXT"
    case UuidSQLType => "UUID"
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

  val DefaultReserved = Set("user", "begin", "end")

  val defaultEscapeReserved = escapeReserved(DefaultReserved) _
  val hsqldbConfig = JDBCSQLConfig(defaultEscapeReserved, defaultEscapeReserved, hsqlTypeNames, defaultDropTableSQL)
  val postgresConfig = JDBCSQLConfig[PostgresColumn](defaultEscapeReserved, defaultEscapeReserved, pgsqlTypeNames,
    t => s"DROP TABLE IF EXISTS $t CASCADE")
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
