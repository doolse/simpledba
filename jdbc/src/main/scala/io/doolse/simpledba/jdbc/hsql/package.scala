package io.doolse.simpledba.jdbc

import java.sql.JDBCType._
import java.sql.{JDBCType, SQLType}
import java.util.UUID

import fs2.Stream
import io.doolse.simpledba.{WriteOp, jdbc}
import io.doolse.simpledba.jdbc.StandardJDBC._

package object hsql {
  case class HSQLColumn[A](wrapped: StdJDBCColumn[A], columnType: ColumnType) extends WrappedColumn[A]

//  case INTEGER => "INTEGER"
//  case BIGINT => "BIGINT"
//  case BOOLEAN => "BOOLEAN"
//  case SMALLINT => "SMALLINT"
//  case FLOAT => "FLOAT"
//  case DOUBLE => "DOUBLE"
//  case TIMESTAMP => "TIMESTAMP"
//  case NVARCHAR => "NVARCHAR"
//
  object HSQLColumn
  {
    implicit val uuidCol = HSQLColumn[UUID](StdJDBCColumn.uuidCol(JDBCType.NVARCHAR), ColumnType("UUID"))

    implicit val longCol = HSQLColumn[Long](StdJDBCColumn.longCol, ColumnType("BIGINT"))

    implicit val intCol = HSQLColumn[Int](StdJDBCColumn.intCol, ColumnType("INTEGER"))

    implicit val stringCol = HSQLColumn[String](StdJDBCColumn.stringCol, ColumnType("LONGVARCHAR"))

    implicit val boolCol = HSQLColumn[Boolean](StdJDBCColumn.boolCol, ColumnType("BOOLEAN"))
  }

  val hsqldbConfig = JDBCSQLConfig[HSQLColumn](defaultEscapeReserved, defaultEscapeReserved, stdSQLQueries, stdExpressionSQL,
    stdTypeNames, HSQLSchemaSQL.apply)

  case class HSQLSchemaSQL(config: JDBCConfig) extends StandardSchemaSQL(config)

}
