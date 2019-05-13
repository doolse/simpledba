package io.doolse.simpledba.jdbc

import java.sql.JDBCType._
import java.sql.{JDBCType, SQLType}
import java.time.Instant
import java.util.UUID

import fs2.Stream
import io.doolse.simpledba.{WriteOp, jdbc}
import io.doolse.simpledba.jdbc.StandardJDBC._

package object hsql {
  case class HSQLColumn[A](wrapped: StdJDBCColumn[A], columnType: ColumnType)
      extends WrappedColumn[A]

//  case INTEGER => "INTEGER"
//  case BIGINT => "BIGINT"
//  case BOOLEAN => "BOOLEAN"
//  case SMALLINT => "SMALLINT"
//  case FLOAT => "FLOAT"
//  case DOUBLE => "DOUBLE"
//  case TIMESTAMP => "TIMESTAMP"
//  case NVARCHAR => "NVARCHAR"
//
  trait StdHSQLColumns extends StdColumns[HSQLColumn] {
    implicit def uuidCol =
      HSQLColumn[UUID](StdJDBCColumn.uuidCol(JDBCType.NVARCHAR), ColumnType("UUID"))

    implicit def longCol = HSQLColumn[Long](StdJDBCColumn.longCol, ColumnType("BIGINT"))

    implicit def intCol = HSQLColumn[Int](StdJDBCColumn.intCol, ColumnType("INTEGER"))

    implicit def stringCol = HSQLColumn[String](StdJDBCColumn.stringCol, ColumnType("LONGVARCHAR"))

    implicit def boolCol = HSQLColumn[Boolean](StdJDBCColumn.boolCol, ColumnType("BOOLEAN"))

    override def wrap[A, B](col: HSQLColumn[A],
                            edit: StdJDBCColumn[A] => StdJDBCColumn[B],
                            editType: ColumnType => ColumnType): HSQLColumn[B] =
      col.copy(wrapped = edit(col.wrapped), columnType = editType(col.columnType))

    override def sizedStringType(size: Int): String = ???

    override implicit def floatCol: HSQLColumn[Float] = ???

    override implicit def doubleCol: HSQLColumn[Double] = ???

    override implicit def instantCol: HSQLColumn[Instant] =
      HSQLColumn[Instant](StdJDBCColumn.instantCol, ColumnType("TIMESTAMP"))
  }

  object HSQLColumn extends StdHSQLColumns

  val hsqldbConfig = JDBCSQLConfig[HSQLColumn](
    defaultEscapeReserved,
    defaultEscapeReserved,
    stdSQLQueries,
    stdExpressionSQL,
    stdTypeNames,
    HSQLSchemaSQL.apply
  )

  case class HSQLSchemaSQL(config: JDBCConfig) extends StandardSchemaSQL(config)

}
