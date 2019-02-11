package io.doolse.simpledba.jdbc

import java.sql.JDBCType.{SQLXML => _}

object StandardJDBC {

  abstract class StandardSchemaSQL(config: JDBCConfig) extends JDBCSchemaSQL {
    def col(b: NamedColumn): String = config.escapeColumnName(b.name)

    def dropTable(t: TableDefinition) =
      s"DROP TABLE ${config.escapeTableName(t.name)} IF EXISTS"

    def createTable(t: TableDefinition): String = {
      val colStrings = t.columns.map { cb =>
        s"${col(cb)} ${config.typeName(cb.columnType, t.primaryKey.contains(cb.name))}${if (!cb.columnType.nullable) " NOT NULL"
        else ""}"
      }
      val withPK = colStrings :+ s"PRIMARY KEY${brackets(t.primaryKey.map(config.escapeColumnName))}"
      s"CREATE TABLE ${config.escapeTableName(t.name)} ${brackets(withPK)}"
    }

    def addColumns(t: TableColumns): Seq[String] = {
      def mkAddCol(cb: NamedColumn) =
        s"ADD COLUMN ${col(cb)} ${config.typeName(cb.columnType, false)}"

      Seq(s"ALTER TABLE ${config.escapeTableName(t.name)} ${t.columns.map(mkAddCol).mkString(",")}")
    }
    def truncateTable(t: TableDefinition): String =
      s"TRUNCATE TABLE ${config.escapeTableName(t.name)}"

    override def createIndex(t: TableColumns, named: String): String =
      s"CREATE INDEX ${config.escapeColumnName(named)} on ${config.escapeTableName(t.name)} ${t.columns
        .map(col)
        .mkString("(", ",", ")")}"
  }

  def stdTypeNames(sql: ColumnType, key: Boolean): String =
    sql.typeName

  val DefaultReserved = Set("user", "begin", "end", "order", "by", "select", "from", "count")

  val defaultEscapeReserved = escapeReserved(DefaultReserved) _
  val defaultParam          = (_: NamedColumn) => "?"

  def escapeReserved(rw: Set[String])(s: String): String = {
    val lc = s.toLowerCase
    if (rw.contains(lc) || lc != s) '"' + s + '"' else s
  }

  def brackets(c: Iterable[String]): String = c.mkString("(", ",", ")")

  def stdExpressionSQL(c: JDBCConfig)(expr: SQLExpression): String = {
    expr match {
      case ColumnReference(name) => c.escapeColumnName(name.name)
      case FunctionCall(name, params) =>
        s"$name${params.map(stdExpressionSQL(c)).mkString("(", ",", ")")}"
      case SQLString(s)                       => s"'$s'"
      case Parameter(sqlType)                 => "?"
      case Aggregate(AggregateOp.Count, None) => "count(*)"
    }
  }

  def stdWhereClause(expr: SQLExpression => String, w: Seq[JDBCWhereClause]): String = {
    def clauseToString(c: JDBCWhereClause) = c match {
      case BinClause(left, op, right) =>
        val opString = op match {
          case BinOp.EQ   => "="
          case BinOp.GT   => ">"
          case BinOp.GTE  => ">="
          case BinOp.LT   => "<"
          case BinOp.LTE  => "<="
          case BinOp.LIKE => "LIKE"
        }
        s"${expr(left)} $opString ${expr(right)}"
    }

    if (w.isEmpty) "" else s"WHERE ${w.map(clauseToString).mkString(" AND ")}"
  }

  def stdOrderBy(c: JDBCConfig, oc: Seq[(NamedColumn, Boolean)]): String = {
    def orderClause(t: (NamedColumn, Boolean)) =
      s"${c.escapeColumnName(t._1.name)} ${if (t._2) "ASC" else "DESC"}"

    if (oc.isEmpty) "" else s"ORDER BY ${oc.map(orderClause).mkString(",")}"
  }

  def stdSQLQueries(mc: JDBCConfig)(q: JDBCPreparedQuery): String = {

    val expr                      = mc.exprToSQL
    def escapeCol(c: NamedColumn) = mc.escapeColumnName(c.name)

    def returning(ret: Seq[NamedColumn]) = {
      if (ret.isEmpty) ""
      else {
        s" RETURNING ${ret.map(escapeCol).mkString(",")}"
      }
    }
    q match {
      case JDBCSelect(t, prj, w, o, l) =>
        s"SELECT ${prj.map(p => expr(p.sql)).mkString(",")} FROM ${mc.escapeTableName(t)} ${stdWhereClause(expr, w)} ${stdOrderBy(mc, o)}"
      case JDBCInsert(t, c) =>
        s"INSERT INTO ${mc.escapeTableName(t)} ${brackets(c.map(v => escapeCol(v.column)))} VALUES ${brackets(
          c.map(v => expr(v.expression))
        )}"
      case JDBCDelete(t, w) =>
        s"DELETE FROM ${mc.escapeTableName(t)} ${stdWhereClause(expr, w)}"
      case JDBCUpdate(t, a, w) =>
        val asgns = a.map(c => s"${escapeCol(c.column)} = ${expr(c.expression)}")
        s"UPDATE ${mc.escapeTableName(t)} SET ${asgns.mkString(",")} ${stdWhereClause(expr, w)}"
      case JDBCRawSQL(sql) => sql
    }
  }

}
