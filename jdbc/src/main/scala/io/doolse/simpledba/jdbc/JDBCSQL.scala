package io.doolse.simpledba.jdbc

import java.sql.SQLType

import io.doolse.simpledba.WriteOp

trait JDBCConfig {

  type C[A] <: JDBCColumn

  def escapeTableName: String => String
  def escapeColumnName: String => String
  def createParamString: JDBCColumnBinding => String
  def logPrepare: String => Unit
  def logBind: (() => (String, Seq[BindLog])) => Unit
}

case class JDBCSQLConfig[C0[_] <: JDBCColumn]
(escapeTableName: String => String, escapeColumnName: String => String,
 createParamString: JDBCColumnBinding => String,
 logPrepare: String => Unit = _ => (),
 logBind: (() => (String, Seq[BindLog])) => Unit = _ => ()) extends JDBCConfig
{
  type C[A] = C0[A]
  def withPrepareLogger(l: String => Unit): JDBCSQLConfig[C0] = copy(logPrepare = l)
  def withBindingLogger(l: (() => (String, Seq[BindLog])) => Unit): JDBCSQLConfig[C0] = copy(logBind = l)
}

sealed trait BindLog
case class WhereBinding(vals: Seq[Any]) extends BindLog
case class UpdateBinding(vals: Seq[Any]) extends BindLog

case class JDBCTableDefinition(name: String, columns: Seq[JDBCColumnBinding], primaryKey: Seq[String])

sealed trait JDBCPreparedQuery

case class JDBCColumnBinding(name: String, sqlType: SQLType, nullable: Boolean)

object JDBCColumnBinding
{
  def apply[C[_] <: JDBCColumn](p: (String, C[_])): JDBCColumnBinding =
    JDBCColumnBinding(p._1, p._2.sqlType, p._2.nullable)
}

case class JDBCInsert(table: String, columns: Seq[JDBCColumnBinding]) extends JDBCPreparedQuery

case class JDBCUpdate(table: String, assignments: Seq[JDBCColumnBinding], where: Seq[JDBCWhereClause]) extends JDBCPreparedQuery

case class JDBCDelete(table: String, where: Seq[JDBCWhereClause]) extends JDBCPreparedQuery

case class JDBCSelect(table: String, columns: Seq[JDBCColumnBinding], where: Seq[JDBCWhereClause],
                      ordering: Seq[(JDBCColumnBinding, Boolean)], limit: Boolean) extends JDBCPreparedQuery

case class JDBCRawSQL(sql: String) extends JDBCPreparedQuery

sealed trait JDBCWhereClause

case class EQ(column: JDBCColumnBinding) extends JDBCWhereClause

case class GT[A](column: JDBCColumnBinding) extends JDBCWhereClause

case class GTE[A](column: JDBCColumnBinding) extends JDBCWhereClause

case class LT[A](column: JDBCColumnBinding) extends JDBCWhereClause

case class LTE[A](column: JDBCColumnBinding) extends JDBCWhereClause

object JDBCPreparedQuery {

  def brackets(c: Iterable[String]): String = c.mkString("(", ",", ")")

  def asSQL[C[_]](q: JDBCPreparedQuery, mc: JDBCConfig) : String = {

    def escapeCol(c: JDBCColumnBinding) = mc.escapeColumnName(c.name)

    def whereClause(w: Seq[JDBCWhereClause]): String = {
      def singleCC(c: JDBCColumnBinding, op: String) = s"${escapeCol(c)} $op ${mc.createParamString(c)}"

      def clauseToString(c: JDBCWhereClause) = c match {
        case EQ(column) => singleCC(column, "=")
        case GT(column) => singleCC(column, ">")
        case GTE(column) => singleCC(column, ">=")
        case LT(column) => singleCC(column, "<")
        case LTE(column) => singleCC(column, "<=")
      }

      if (w.isEmpty) "" else s"WHERE ${w.map(clauseToString).mkString(" AND ")}"
    }

    def orderBy(oc: Seq[(JDBCColumnBinding, Boolean)]): String = {
      def orderClause(t: (JDBCColumnBinding, Boolean)) = s"${escapeCol(t._1)} ${if (t._2) "ASC" else "DESC"}"

      if (oc.isEmpty) "" else s"ORDER BY ${oc.map(orderClause).mkString(",")}"
    }


    q match {
      case JDBCSelect(t, c, w, o, l) =>
        s"SELECT ${c.map(escapeCol).mkString(",")} FROM ${mc.escapeTableName(t)} ${whereClause(w)} ${orderBy(o)}"
      case JDBCInsert(t, c) =>
        s"INSERT INTO ${mc.escapeTableName(t)} ${brackets(c.map(escapeCol))} VALUES ${brackets(c.map(mc.createParamString))}"
      case JDBCDelete(t, w) =>
        s"DELETE FROM ${mc.escapeTableName(t)} ${whereClause(w)}"
      case JDBCUpdate(t, a, w) =>
        val asgns = a.map(c => s"${escapeCol(c)} = ${mc.createParamString(c)}")
        s"UPDATE ${mc.escapeTableName(t)} SET ${asgns.mkString(",")} ${whereClause(w)}"
      case JDBCRawSQL(sql) => sql
    }
  }
}

case class JDBCWriteOp(q: JDBCPreparedQuery, config: JDBCConfig, binder: BindFunc[Seq[BindLog]]) extends WriteOp

