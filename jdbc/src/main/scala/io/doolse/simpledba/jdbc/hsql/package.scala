package io.doolse.simpledba.jdbc

import java.sql.JDBCType
import java.time.Instant
import java.util.UUID

package object hsql {
  case class HSQLColumn[A](wrapped: StdJDBCColumn[A], columnType: ColumnType)
      extends WrappedColumn[A]

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

    override def sizedStringType(size: Int): String = s"VARCHAR($size)"

    override implicit def floatCol: HSQLColumn[Float] =
      HSQLColumn[Float](StdJDBCColumn.floatCol, ColumnType("FLOAT"))

    override implicit def doubleCol: HSQLColumn[Double] =
      HSQLColumn[Double](StdJDBCColumn.doubleCol, ColumnType("DOUBLE"))

    override implicit def instantCol: HSQLColumn[Instant] =
      HSQLColumn[Instant](StdJDBCColumn.instantCol, ColumnType("TIMESTAMP"))
  }

  object HSQLColumn extends StdHSQLColumns

  trait HSQLDialect extends StdSQLDialect

  object HSQLDialect extends HSQLDialect

  val hsqldbMapper = JDBCMapper[HSQLColumn](HSQLDialect)

}
