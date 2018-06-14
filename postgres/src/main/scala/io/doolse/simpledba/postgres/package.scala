package io.doolse.simpledba

import java.sql.JDBCType.{DOUBLE, FLOAT, LONGNVARCHAR}
import java.sql.SQLType

import io.doolse.simpledba.jdbc.Dialects.{defaultEscapeColumn, defaultEscapeReserved, stdSQLTypeNames}
import io.doolse.simpledba.jdbc.{JDBCColumnBinding, JDBCSQLConfig, UuidSQLType}

package object postgres {

  val pgsqlTypeNames : SQLType => String = ({
    case FLOAT => "REAL"
    case DOUBLE => "DOUBLE PRECISION"
    case LONGNVARCHAR => "TEXT"
    case UuidSQLType => "UUID"
  } : PartialFunction[SQLType, String]) orElse stdSQLTypeNames

  def createPostgresParam(c: JDBCColumnBinding): String = {
    c.sqlType match {
      case JSONBType => "?::JSONB"
      case _ => "?"
    }
  }

  val postgresConfig = JDBCSQLConfig[PostgresColumn](defaultEscapeReserved, defaultEscapeColumn,
    createPostgresParam, pgsqlTypeNames,
    t => s"DROP TABLE IF EXISTS $t CASCADE")

}
