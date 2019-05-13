package io.doolse.simpledba.jdbc

import java.sql.SQLType

import io.doolse.simpledba.{ColumnRecord, WriteOp}
import io.doolse.simpledba.jdbc.AggregateOp.AggregateOp
import io.doolse.simpledba.jdbc.BinOp.BinOp
import shapeless.HList

trait JDBCSchemaSQL {
  def dropTable(t: TableDefinition): String
  def createTable(t: TableDefinition): String
  def addColumns(t: TableColumns): Seq[String]
  def truncateTable(t: TableDefinition): String
  def createIndex(t: TableColumns, named: String): String
}

trait JDBCConfig {

  type C[A] <: JDBCColumn

  def escapeTableName: String => String
  def escapeColumnName: String => String
  def queryToSQL: JDBCPreparedQuery => String
  def exprToSQL: SQLExpression => String
  def typeName: (ColumnType, Boolean) => String
  def logPrepare: String => Unit
  def schemaSQL: JDBCSchemaSQL
  def logBind: (String, Seq[BindLog]) => Unit

  def record[R <: HList](implicit cr: ColumnRecord[C, Unit, R]): ColumnRecord[C, Unit, R] = cr

}

class ColumnRecordCreate

object JDBCConfig {
  type Aux[C0[_] <: JDBCColumn] = JDBCConfig {
    type C[A] = C0[A]
  }
}

case class JDBCSQLConfig[C0[_] <: JDBCColumn](
    escapeTableName: String => String,
    escapeColumnName: String => String,
    _queryString: JDBCConfig => JDBCPreparedQuery => String,
    _exprToSQL: JDBCConfig => SQLExpression => String,
    typeName: (ColumnType, Boolean) => String,
    _schemaSQL: JDBCConfig => JDBCSchemaSQL,
    logPrepare: String => Unit = _ => (),
    logBind: (String, Seq[BindLog]) => Unit = (_,_) => ()
) extends JDBCConfig {
  type C[A] = C0[A]
  def withPrepareLogger(l: String => Unit): JDBCSQLConfig[C0] = copy(logPrepare = l)
  def withBindingLogger(l: (String, Seq[BindLog]) => Unit): JDBCSQLConfig[C0] =
    copy(logBind = l)
  def withTypeName(n: (ColumnType, Boolean) => String): JDBCSQLConfig[C0] = copy(typeName = n)
  def schemaSQL                                                           = _schemaSQL(this)
  val queryToSQL                                                          = _queryString(this)
  val exprToSQL                                                           = _exprToSQL(this)
}

sealed trait BindLog
case class WhereLog(vals: Seq[Any]) extends BindLog
case class ValueLog(vals: Seq[Any]) extends BindLog

case class TableDefinition(name: String, columns: Seq[NamedColumn], primaryKey: Seq[String])

case class TableColumns(name: String, columns: Seq[NamedColumn])

case class NamedColumn(name: String, columnType: ColumnType)

object NamedColumn {
  def apply[C[_] <: JDBCColumn](p: (String, C[_])): NamedColumn =
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

case class JDBCWriteOp(q: JDBCPreparedQuery, config: JDBCConfig, binder: BindFunc[Seq[BindLog]])
    extends WriteOp
