package io.doolse.simpledba.jdbc

import java.sql.SQLType
import java.sql.JDBCType._

/**
  * Created by jolz on 12/03/17.
  */

case class JDBCMapperConfig(escapeTableName: String => String, escapeColumnName: String => String, sqlTypeToString: SQLType => String)

object JDBCMapperConfig {
  val stdSQLTypeNames : PartialFunction[SQLType, String] = {
    case t if t.getVendor == "java.sql" => t match {
      case INTEGER => "INT"
      case LONGNVARCHAR => "LONGVARCHAR"
    }
    case o => o.getName
  }
  val DefaultReserved = Set("user")

  val defaultJDBCMapperConfig = JDBCMapperConfig(escapeReserved(DefaultReserved), escapeReserved(DefaultReserved), stdSQLTypeNames)

  def escapeReserved(rw: Set[String])(s: String): String = {
    val lc = s.toLowerCase
    if (rw.contains(lc) || lc != s) '"' + s + '"' else s
  }

}
