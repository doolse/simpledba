package io.doolse.simpledba.jdbc

import java.sql.SQLType

trait JDBCConfig {

  type C[A] <: JDBCColumn

  def escapeTableName: String => String
  def escapeColumnName: String => String
  def sqlTypeToString: SQLType => String
  def dropTable: String => String
  def logPrepare: String => Unit
  def logBind: (() => (String, Seq[BindLog])) => Unit

  def dropTableSQL(dt: JDBCDropTable) : String = dropTable(escapeTableName(dt.name))
}

case class JDBCSQLConfig[C0[_] <: JDBCColumn]
(escapeTableName: String => String, escapeColumnName: String => String,
 sqlTypeToString: SQLType => String, dropTable: String => String,
 logPrepare: String => Unit = _ => (),
 logBind: (() => (String, Seq[BindLog])) => Unit = _ => ()) extends JDBCConfig
{
  type C[A] = C0[A]
  def withBindingLogger(l: (() => (String, Seq[BindLog])) => Unit) = copy[C0](logBind = l)
}

sealed trait BindLog
case class WhereBinding(vals: Seq[Any]) extends BindLog
case class UpdateBinding(vals: Seq[Any]) extends BindLog

sealed trait JDBCPreparedQuery

case class JDBCDropTable(name: String) extends JDBCPreparedQuery

case class JDBCCreateTable(name: String, columns: Seq[(String, Boolean, SQLType)], primaryKey: Seq[String]) extends JDBCPreparedQuery

case class JDBCTruncate(name: String) extends JDBCPreparedQuery

case class JDBCInsert(table: String, columns: Seq[String]) extends JDBCPreparedQuery

case class JDBCUpdate(table: String, assignments: Seq[String], where: Seq[JDBCWhereClause]) extends JDBCPreparedQuery

case class JDBCDelete(table: String, where: Seq[JDBCWhereClause]) extends JDBCPreparedQuery

case class JDBCSelect(table: String, columns: Seq[String], where: Seq[JDBCWhereClause], ordering: Seq[(String, Boolean)], limit: Boolean) extends JDBCPreparedQuery

case class JDBCRawSQL(sql: String) extends JDBCPreparedQuery

sealed trait JDBCWhereClause

case class EQ(column: String) extends JDBCWhereClause

case class GT[A](column: String) extends JDBCWhereClause

case class GTE[A](column: String) extends JDBCWhereClause

case class LT[A](column: String) extends JDBCWhereClause

case class LTE[A](column: String) extends JDBCWhereClause

object JDBCPreparedQuery {

  def brackets(c: Iterable[String]): String = c.mkString("(", ",", ")")

  def asSQL[C[_]](q: JDBCPreparedQuery, mc: JDBCConfig) : String = {
    def whereClause(w: Seq[JDBCWhereClause]): String = {
      def singleCC(c: String, op: String) = s"${mc.escapeColumnName(c)} ${op} ?"

      def clauseToString(c: JDBCWhereClause) = c match {
        case EQ(column) => singleCC(column, "=")
        case GT(column) => singleCC(column, ">")
        case GTE(column) => singleCC(column, ">=")
        case LT(column) => singleCC(column, "<")
        case LTE(column) => singleCC(column, "<=")
      }

      if (w.isEmpty) "" else s"WHERE ${w.map(clauseToString).mkString(" AND ")}"
    }

    def orderBy(oc: Seq[(String, Boolean)]): String = {
      def orderClause(t: (String, Boolean)) = s"${mc.escapeColumnName(t._1)} ${if (t._2) "ASC" else "DESC"}"

      if (oc.isEmpty) "" else s"ORDER BY ${oc.map(orderClause).mkString(",")}"
    }

    q match {
      case JDBCSelect(t, c, w, o, l) => s"SELECT ${c.map(mc.escapeColumnName).mkString(",")} FROM ${mc.escapeTableName(t)} ${whereClause(w)} ${orderBy(o)}"
      case JDBCInsert(t, c) => s"INSERT INTO ${mc.escapeTableName(t)} ${brackets(c.map(mc.escapeColumnName))} VALUES ${brackets(c.map(_ => "?"))}"
      case JDBCDelete(t, w) => s"DELETE FROM ${mc.escapeTableName(t)} ${whereClause(w)}"
      case JDBCUpdate(t, a, w) =>
        val asgns = a.map(c => s"${mc.escapeColumnName(c)} = ?")
        s"UPDATE ${mc.escapeTableName(t)} SET ${asgns.mkString(",")} ${whereClause(w)}"
      case JDBCRawSQL(sql) => sql
      case JDBCCreateTable(t, c, pk) =>
        val colStrings = c.map {
          case (cn, nullable, ct) => s"${mc.escapeColumnName(cn)} ${mc.sqlTypeToString(ct)}${if (!nullable) " NOT NULL" else ""}"
        }
        val withPK = colStrings :+ s"PRIMARY KEY${brackets(pk.map(mc.escapeColumnName))}"
        s"CREATE TABLE ${mc.escapeTableName(t)} ${brackets(withPK)}"
      case JDBCTruncate(t) => s"TRUNCATE TABLE ${mc.escapeTableName(t)}"
      case dt: JDBCDropTable => mc.dropTableSQL(dt)
    }
  }
}
