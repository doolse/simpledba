package io.doolse.simpledba2.jdbc

class JDBCSQLConfig(val escapeTableName: String => String,
                    val escapeColumnName: String => String)
{
  type C[A] <: JDBCColumn
}

sealed trait JDBCPreparedQuery

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

  def asSQL[C[_]](q: JDBCPreparedQuery, mc: JDBCSQLConfig) = {
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
    }
  }
}
