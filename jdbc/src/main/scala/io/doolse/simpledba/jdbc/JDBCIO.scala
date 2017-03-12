package io.doolse.simpledba.jdbc

import java.sql._
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import fs2._
import io.doolse.simpledba.{ColumnMapping, ColumnMaterialzer, PhysicalValue}
import JDBCUtils.brackets
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by jolz on 12/03/17.
  */

object JDBCIO {

  def openConnection(config: Config = ConfigFactory.load()) = {
    val jdbcConfig = config.getConfig("simpledba.jdbc")
    val jdbcUrl = jdbcConfig.getString("url")
    val con = if (jdbcConfig.hasPath("credentials")) {
      val cc = jdbcConfig.getConfig("credentials")
      DriverManager.getConnection(jdbcUrl, cc.getString("username"), cc.getString("password"))
    } else DriverManager.getConnection(jdbcUrl)
    JDBCSession(con, JDBCMapperConfig.defaultJDBCMapperConfig)
  }

  def rowsStream[F[_]](rs: ResultSet): Stream[F, ResultSet] = {
    if (!rs.next) Stream.empty[F, ResultSet]
    else Stream.emit(rs) ++ Stream.suspend(rowsStream(rs))
  }

  def rowMaterializer(r: ResultSet) = new ColumnMaterialzer[JDBCColumn] {
    def apply[A](name: String, atom: JDBCColumn[A]): A = atom.byName(r, name).get
  }

}

case class JDBCSession(connection: Connection, config: JDBCMapperConfig, logger: (() ⇒ String) ⇒ Unit = _ => (),
                            statementCache : scala.collection.concurrent.Map[Any, Task[PreparedStatement]]
                            = new ConcurrentHashMap[Any, Task[PreparedStatement]]().asScala) {

  def prepare(q: JDBCPreparedQuery): Task[PreparedStatement] = {
    lazy val prepared = {
      logger(() => "Preparing: "+JDBCPreparedQuery.asSQL(q, config))
      Task.now(connection.prepareStatement(JDBCPreparedQuery.asSQL(q, config)))
    }
    statementCache.getOrElseUpdate(q, prepared)
  }

  def prepareAndBind(q: JDBCPreparedQuery, s: Seq[PhysicalValue[JDBCColumn]]): Task[PreparedStatement] = {
    prepare(q).map { ps =>
      logger {
        () =>
          val vals = s.map(pv => pv.name+"="+pv.v).mkString(",")
          "Statement: " + JDBCPreparedQuery.asSQL(q, config) + " with values " + vals
      }
      s.zipWithIndex.foreach {
        case (pv, i) => pv.atom.bind(ps, i+1, pv.v)
      }
      ps
    }
  }

  def execQuery(q: JDBCPreparedQuery, s: Seq[PhysicalValue[JDBCColumn]]): Task[ResultSet] = prepareAndBind(q, s).map(_.executeQuery())
  def execWrite(q: JDBCPreparedQuery, s: Seq[PhysicalValue[JDBCColumn]]): Task[Boolean] = prepareAndBind(q, s).map(_.execute())
}

sealed trait JDBCPreparedQuery
case class JDBCCreateTable(name: String, columns: Seq[(String, SQLType)], primaryKey: Seq[String]) extends JDBCPreparedQuery
case class JDBCInsert(table:String, columns: Seq[String]) extends JDBCPreparedQuery
case class JDBCUpdate(table:String, assignments: Seq[String], where: Seq[JDBCWhereClause]) extends JDBCPreparedQuery
case class JDBCDelete(table: String, where: Seq[JDBCWhereClause]) extends JDBCPreparedQuery
case class JDBCSelect(table: String, columns: Seq[String], where: Seq[JDBCWhereClause], ordering: Seq[(String, Boolean)], limit: Boolean) extends JDBCPreparedQuery

sealed trait JDBCWhereClause
case class EQ(column: String) extends JDBCWhereClause
case class GT(column: String) extends JDBCWhereClause
case class GTE(column: String) extends JDBCWhereClause
case class LT(column: String) extends JDBCWhereClause
case class LTE(column: String) extends JDBCWhereClause

object JDBCPreparedQuery {


  def whereClause(w: Seq[JDBCWhereClause], mc: JDBCMapperConfig): String = {
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

  def asSQL(q: JDBCPreparedQuery, mc: JDBCMapperConfig) = q match {
    case JDBCSelect(t, c, w, o, l) => s"SELECT ${c.map(mc.escapeColumnName).mkString(",")} FROM ${mc.escapeTableName(t)} ${whereClause(w, mc)}"
    case JDBCInsert(t, c) => s"INSERT INTO ${mc.escapeTableName(t)} ${brackets(c.map(mc.escapeColumnName))} VALUES ${brackets(c.map(_ => "?"))}"
    case JDBCDelete(t, w) => s"DELETE FROM ${mc.escapeTableName(t)} ${whereClause(w, mc)}"
    case JDBCUpdate(t, a, w) =>
      val asgns = a.map(c => s"${mc.escapeColumnName(c)} = ?")
      s"UPDATE ${mc.escapeTableName(t)} SET ${asgns.mkString(",")} ${whereClause(w, mc)}"
    case JDBCCreateTable(t, c, pk) =>
      val colStrings = c.map {
        case (cn, ct) => s"${mc.escapeColumnName(cn)} ${mc.sqlTypeToString(ct)}"
      }
      val withPK = colStrings :+ s"PRIMARY KEY${brackets(pk.map(mc.escapeColumnName))}"
      s"CREATE TABLE ${mc.escapeTableName(t)} ${brackets(withPK)}"
  }

  def exactMatch[T](s: Seq[ColumnMapping[JDBCColumn, T, _]]) = s.map(c => EQ(c.name))

}

