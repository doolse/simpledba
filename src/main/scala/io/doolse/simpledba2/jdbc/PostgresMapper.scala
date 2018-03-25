package io.doolse.simpledba2.jdbc

import java.sql.JDBCType.{SQLXML => _, _}
import java.sql._
import java.util.UUID

case class PostgresColumn[AA](sqlType: SQLType, getByIndex: (Int, ResultSet) => Option[AA],
                             bind: (Int, AA, Connection, PreparedStatement) => Unit) extends JDBCColumn
{
  type A = AA
  override def nullable: Boolean = ???

}

object PostgresColumn
{
  def temp[A](r: (Int, ResultSet) => Option[A],
              b: (Int, A, Connection, PreparedStatement) => Unit) = PostgresColumn[A](JDBCType.NVARCHAR, r, b)

  implicit val stringCol = temp[String]((i,rs) => Option(rs.getString(i)),
    (i,v,_,ps) => ps.setString(i, v))
  implicit val intCol = temp[Int]((i, rs) =>
    Option(rs.getInt(i)).filterNot(_ => rs.wasNull),
    (i,v,_,ps) => ps.setInt(i, v))
  implicit val longCol = temp[Long]((i, rs) =>
    Option(rs.getLong(i)).filterNot(_ => rs.wasNull),
    (i,v,_,ps) => ps.setLong(i, v))
  implicit val boolCol = temp[Boolean]((i, rs) =>
    Option(rs.getBoolean(i)).filterNot(_ => rs.wasNull),
    (i,v,_,ps) => ps.setBoolean(i, v))
  implicit val doubleCol : PostgresColumn[Double] = temp[Double](
    (i, rs) => Option(rs.getDouble(i)).filterNot(_ => rs.wasNull()),
    (i, v, _, ps) => ps.setDouble(i, v))
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
