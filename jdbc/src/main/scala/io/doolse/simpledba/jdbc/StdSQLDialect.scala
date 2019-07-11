package io.doolse.simpledba.jdbc

object StdSQLDialect {
  def brackets(c: Iterable[String]): String = c.mkString("(", ",", ")")
}

trait StdSQLDialect extends SQLDialect {

  import StdSQLDialect._
  override def querySQL(query: JDBCPreparedQuery): String = stdQuerySQL(query)

  override def maxInParamaters: Int = 100
  def expressionSQL(expression: SQLExpression): String    = stdExpressionSQL(expression)
  def typeName(c: ColumnType, keyColumn: Boolean): String = stdTypeName(c, keyColumn)
  def orderBy(oc: Seq[(NamedColumn, Boolean)]): String    = stdOrderBy(oc)
  def whereClause(w: Seq[JDBCWhereClause]): String        = stdWhereClause(w)

  protected final def stdExpressionSQL(expr: SQLExpression): String = {
    expr match {
      case ColumnReference(name) => escapeColumnName(name.name)
      case FunctionCall(name, params) =>
        s"$name${brackets(params.map(expressionSQL))}"
      case SQLString(s)                       => s"'$s'"
      case Parameter(sqlType)                 => "?"
      case Aggregate(AggregateOp.Count, None) => "count(*)"
      case Expressions(exprs) => brackets(exprs.map(expressionSQL))
    }
  }

  protected def stdTypeName(sql: ColumnType, key: Boolean): String =
    sql.typeName

  val DefaultReserved = Set("user", "begin", "end", "order", "by", "select", "from", "count")

  def reservedIdentifiers = DefaultReserved

  val defaultParam = (_: NamedColumn) => "?"

  def escapeColumnName(name: String): String = escapeReserved(reservedIdentifiers)(name)

  def escapeTableName(name: String): String = escapeReserved(reservedIdentifiers)(name)

  def escapeReserved(rw: Set[String])(s: String): String = {
    val lc = s.toLowerCase
    if (rw.contains(lc) || lc != s) '"' + s + '"' else s
  }

  protected final def stdOrderBy(oc: Seq[(NamedColumn, Boolean)]): String = {
    def orderClause(t: (NamedColumn, Boolean)) =
      s"${escapeColumnName(t._1.name)} ${if (t._2) "ASC" else "DESC"}"

    if (oc.isEmpty) "" else s"ORDER BY ${oc.map(orderClause).mkString(",")}"
  }

  protected final def stdWhereClause(w: Seq[JDBCWhereClause]): String = {
    def clauseToString(c: JDBCWhereClause) = c match {
      case BinClause(left, op, right) =>
        val opString = op match {
          case BinOp.EQ   => "="
          case BinOp.GT   => ">"
          case BinOp.GTE  => ">="
          case BinOp.LT   => "<"
          case BinOp.LTE  => "<="
          case BinOp.LIKE => "LIKE"
          case BinOp.IN => "IN"
          case BinOp.CONTAINS => "@>"
        }
        s"${expressionSQL(left)} $opString ${expressionSQL(right)}"
    }

    if (w.isEmpty) "" else s"WHERE ${w.map(clauseToString).mkString(" AND ")}"
  }

  protected final def stdQuerySQL(query: JDBCPreparedQuery): String = {

    def escapeCol(c: NamedColumn) = escapeColumnName(c.name)

    def returning(ret: Seq[NamedColumn]) = {
      if (ret.isEmpty) ""
      else {
        s" RETURNING ${ret.map(escapeCol).mkString(",")}"
      }
    }
    query match {
      case JDBCSelect(t, prj, w, o, l) =>
        s"SELECT ${prj.map(p => expressionSQL(p.sql)).mkString(",")} FROM ${escapeTableName(t)} ${whereClause(
          w)} ${orderBy(o)}"
      case JDBCInsert(t, c) =>
        s"INSERT INTO ${escapeTableName(t)} ${brackets(c.map(v => escapeCol(v.column)))} VALUES ${brackets(
          c.map(v => expressionSQL(v.expression))
        )}"
      case JDBCDelete(t, w) =>
        s"DELETE FROM ${escapeTableName(t)} ${whereClause(w)}"
      case JDBCUpdate(t, a, w) =>
        val asgns = a.map(c => s"${escapeCol(c.column)} = ${expressionSQL(c.expression)}")
        s"UPDATE ${escapeTableName(t)} SET ${asgns.mkString(",")} ${whereClause(w)}"
      case JDBCRawSQL(sql) => sql
    }
  }

  protected def col(b: NamedColumn): String = escapeColumnName(b.name)

  def dropTable(t: TableDefinition) =
    s"DROP TABLE ${escapeTableName(t.name)} IF EXISTS"

  def createTable(t: TableDefinition): String = {
    val colStrings = t.columns.map { cb =>
      s"${col(cb)} ${typeName(cb.columnType, t.primaryKey.contains(cb.name))}${if (!cb.columnType.nullable) " NOT NULL"
      else ""}"
    }
    val withPK = colStrings :+ s"PRIMARY KEY${brackets(t.primaryKey.map(escapeColumnName))}"
    s"CREATE TABLE ${escapeTableName(t.name)} ${brackets(withPK)}"
  }

  def addColumns(t: TableColumns): Seq[String] = {
    def mkAddCol(cb: NamedColumn) =
      s"ADD COLUMN ${col(cb)} ${typeName(cb.columnType, false)}"

    Seq(s"ALTER TABLE ${escapeTableName(t.name)} ${t.columns.map(mkAddCol).mkString(",")}")
  }
  def truncateTable(t: TableDefinition): String =
    s"TRUNCATE TABLE ${escapeTableName(t.name)}"

  override def createIndex(t: TableColumns, named: String): String =
    s"CREATE INDEX ${escapeColumnName(named)} on ${escapeTableName(t.name)} ${t.columns
      .map(col)
      .mkString("(", ",", ")")}"
}
