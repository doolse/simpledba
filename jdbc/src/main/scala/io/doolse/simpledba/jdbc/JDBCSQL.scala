package io.doolse.simpledba.jdbc

import io.doolse.simpledba.WriteOp
import io.doolse.simpledba.jdbc.AggregateOp.AggregateOp
import io.doolse.simpledba.jdbc.BinOp.BinOp

case class TableDefinition(name: String, columns: Seq[NamedColumn], primaryKey: Seq[String])

case class TableColumns(name: String, columns: Seq[NamedColumn])

case class NamedColumn(name: String, columnType: ColumnType)

object NamedColumn {
  def apply(p: (String, JDBCColumn[_])): NamedColumn =
    NamedColumn(p._1, p._2.columnType)
}

object AggregateOp extends Enumeration {
  type AggregateOp = Value
  val Count = Value
}

object BinOp extends Enumeration {
  type BinOp = Value
  val EQ, GT, GTE, LT, LTE, LIKE = Value
}

sealed trait SQLExpression

case class ColumnReference(name: NamedColumn)                        extends SQLExpression
case class Aggregate(name: AggregateOp, column: Option[NamedColumn]) extends SQLExpression
case class FunctionCall(name: String, params: Seq[SQLExpression])    extends SQLExpression
case class SQLString(s: String)                                      extends SQLExpression
case class Parameter(columnType: ColumnType)                         extends SQLExpression

case class ColumnExpression(column: NamedColumn, expression: SQLExpression)
case class SQLProjection(columnType: ColumnType, sql: SQLExpression)

sealed trait JDBCPreparedQuery
case class JDBCInsert(table: String, values: Seq[ColumnExpression]) extends JDBCPreparedQuery

case class JDBCUpdate(
    table: String,
    assignments: Seq[ColumnExpression],
    where: Seq[JDBCWhereClause]
) extends JDBCPreparedQuery

case class JDBCDelete(table: String, where: Seq[JDBCWhereClause]) extends JDBCPreparedQuery

case class JDBCSelect(
    table: String,
    columns: Seq[SQLProjection],
    where: Seq[JDBCWhereClause],
    ordering: Seq[(NamedColumn, Boolean)],
    limit: Boolean
) extends JDBCPreparedQuery

case class JDBCRawSQL(sql: String) extends JDBCPreparedQuery

sealed trait JDBCWhereClause

case class BinClause(left: SQLExpression, op: BinOp, right: SQLExpression) extends JDBCWhereClause

case class JDBCWriteOp(sql: String, binder: BindFunc[Seq[BindLog]])
    extends WriteOp
