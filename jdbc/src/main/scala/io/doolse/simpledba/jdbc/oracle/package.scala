package io.doolse.simpledba.jdbc

import java.sql.JDBCType.{BIGINT, BOOLEAN, LONGNVARCHAR, NVARCHAR}
import java.sql.SQLType

import fs2.Stream
import io.doolse.simpledba.WriteOp
import io.doolse.simpledba.jdbc.StandardJDBC._

package object oracle {

  case class OracleColumn[AA](wrapped: StdJDBCColumn[AA]) extends WrappedColumn[AA, OracleColumn]
  {
    def mapped[B] = OracleColumn.apply[B]
  }

  object OracleColumn
  {
    implicit def stdCol[A](implicit std: StdJDBCColumn[A]) = OracleColumn(std)
  }

  def oracleTypes(stringKeySize: Int)(cb: JDBCColumnBinding, key: Boolean): String = (cb.sqlType, key) match {
    case (NVARCHAR, _) => "NVARCHAR2"
    case (LONGNVARCHAR, true) => s"NVARCHAR2($stringKeySize)"
    case (LONGNVARCHAR, false) => "NCLOB"
    case (o, _) => o match {
      case BIGINT => "NUMBER(19)"
      case BOOLEAN => "NUMBER(1,0)"
      case os => stdSQLTypeNames(os)
    }

  }

  val oracleReserved = DefaultReserved ++ Set("session")

  val oracleEscapeReserved = escapeReserved(oracleReserved) _

  val oracleConfig = JDBCSQLConfig[OracleColumn](oracleEscapeReserved, oracleEscapeReserved, defaultParam)

  def dropTable(t: JDBCTableDefinition)(implicit config: JDBCConfig): Stream[JDBCIO, WriteOp] =
    rawSQL(s"BEGIN EXECUTE IMMEDIATE 'DROP TABLE ${config.escapeTableName(t.name)}'; EXCEPTION WHEN OTHERS THEN NULL; END;")

  def createTable(t: JDBCTableDefinition, sqlTypeToString: (JDBCColumnBinding, Boolean) => String = oracleTypes(256))(implicit config: JDBCConfig): Stream[JDBCIO, WriteOp] =
    rawSQL(stdCreateTableSQL(t, sqlTypeToString))
}
