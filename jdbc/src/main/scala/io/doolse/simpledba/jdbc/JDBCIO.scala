package io.doolse.simpledba.jdbc

import java.sql._
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import fs2._
import io.doolse.simpledba.{ColumnMapping, ColumnMaterialzer, PhysicalValue}
import JDBCUtils.brackets
import cats.data.{Kleisli, ReaderT, WriterT}
import com.typesafe.config.{Config, ConfigFactory}
import fs2.interop.cats._

/**
  * Created by jolz on 12/03/17.
  */

object JDBCIO {

  def openConnection(config: Config = ConfigFactory.load()) = {
    val jdbcConfig = config.getConfig("simpledba.jdbc")
    val logger = if (!jdbcConfig.hasPath("log")) None else Some((msg: () => String) => Console.out.println(msg()))
    val jdbcUrl = jdbcConfig.getString("url")
    val dialect = jdbcConfig.getString("dialect") match {
      case "hsqldb" => JDBCSQLConfig.hsqldbConfig
      case "postgres" => JDBCSQLConfig.postgresConfig
    }
    val con = if (jdbcConfig.hasPath("credentials")) {
      val cc = jdbcConfig.getConfig("credentials")
      DriverManager.getConnection(jdbcUrl, cc.getString("username"), cc.getString("password"))
    } else DriverManager.getConnection(jdbcUrl)
    JDBCSession(con, dialect, logger = logger.getOrElse(_ => ()))
  }

  def rowsStream[F[_]](rs: ResultSet): Stream[F, ResultSet] = {
    if (!rs.next) Stream.empty[F, ResultSet]
    else Stream.emit(rs) ++ Stream.suspend(rowsStream(rs))
  }

  def rowMaterializer(c: JDBCSession, r: ResultSet) = new ColumnMaterialzer[JDBCColumn] {
    def apply[A](name: String, atom: JDBCColumn[A]): A = atom.byName(c, r, name).getOrElse(sys.error(s"Mapping error - column '$name' contains a NULL"))
  }

  def sessionIO[A](f: JDBCSession => Task[A]): Effect[A] =
    Kleisli(s => WriterT.lift(f(s)))

  def write(f: JDBCWrite): Effect[Unit] = Kleisli.lift(WriterT.tell(Stream(f)))

  def writeSink(s: JDBCSession): Sink[Task, JDBCWrite] = { st =>
    st.rechunkN(1000).chunks.evalMap { chunk =>
      val batches = chunk.map {
        case JDBCWrite(q, v) => (q, v)
      }.toVector.groupBy(_._1).toSeq
      Task.traverse(batches)(b => s.execBatch(b._1, b._2.map(_._2))).map(_ => ())
    }
  }

  def runJDBCWrites[A](fa: JDBCWriter[A]): Kleisli[Task, JDBCSession, A] = Kleisli { s =>
    fa.run.flatMap {
      case (ws, a) => ws.tov(writeSink(s)).run.map(_ => a)
    }
  }

  def runWrites[A](fa: Effect[A]): Kleisli[Task, JDBCSession, A] = Kleisli { s =>
    runJDBCWrites(fa.run(s)).run(s)
  }
}

case class JDBCSession(connection: Connection, config: JDBCSQLConfig, logger: (() ⇒ String) ⇒ Unit = _ => (),
                       statementCache: scala.collection.concurrent.Map[Any, Task[PreparedStatement]]
                       = new ConcurrentHashMap[Any, Task[PreparedStatement]]().asScala) {

  def prepare(q: JDBCPreparedQuery): Task[PreparedStatement] = {
    lazy val prepared = {
      logger(() => "Preparing: " + JDBCPreparedQuery.asSQL(q, config))
      Task.delay(connection.prepareStatement(JDBCPreparedQuery.asSQL(q, config)))
    }
    statementCache.getOrElseUpdate(q, prepared)
  }

  def doBind(ps: PreparedStatement, s: Seq[PhysicalValue[JDBCColumn]]): PreparedStatement = {
    s.zipWithIndex.foreach {
      case (pv, i) => pv.atom.bind(this, ps, i + 1, pv.v)
    }
    ps
  }

  def prepareAndBind(q: JDBCPreparedQuery, s: Seq[PhysicalValue[JDBCColumn]]): Task[PreparedStatement] = {
    prepare(q).map { ps =>
      logger {
        () =>
          val vals = s.map(pv => pv.name + "=" + pv.v).mkString(",")
          "Statement: " + JDBCPreparedQuery.asSQL(q, config) + " with values " + vals
      }
      doBind(ps, s)
    }
  }

  def execQuery(q: JDBCPreparedQuery, s: Seq[PhysicalValue[JDBCColumn]]): Task[ResultSet] = prepareAndBind(q, s).map(_.executeQuery())

  def execWrite(q: JDBCPreparedQuery, s: Seq[PhysicalValue[JDBCColumn]]): Task[Boolean] = prepareAndBind(q, s).map(_.execute())

  def execBatch(q: JDBCPreparedQuery, bvals: Seq[Seq[PhysicalValue[JDBCColumn]]]): Task[Unit] = prepare(q).map { ps =>
    logger(() => s"Batch statement: ${JDBCPreparedQuery.asSQL(q, config)}")
    bvals.foreach { s =>
      logger { () => s"Values ${s.map(pv => pv.name + "=" + pv.v).mkString(",")}" }
      doBind(ps, s).addBatch()
    }
    ps.executeBatch()
    ()
  }
}

case class JDBCWrite(q: JDBCPreparedQuery, s: Seq[PhysicalValue[JDBCColumn]])

sealed trait JDBCPreparedQuery

case class JDBCDropTable(name: String) extends JDBCPreparedQuery

case class JDBCCreateTable(name: String, columns: Seq[(String, SQLType)], primaryKey: Seq[String]) extends JDBCPreparedQuery

case class JDBCTruncate(name: String) extends JDBCPreparedQuery

case class JDBCInsert(table: String, columns: Seq[String]) extends JDBCPreparedQuery

case class JDBCUpdate(table: String, assignments: Seq[String], where: Seq[JDBCWhereClause]) extends JDBCPreparedQuery

case class JDBCDelete(table: String, where: Seq[JDBCWhereClause]) extends JDBCPreparedQuery

case class JDBCSelect(table: String, columns: Seq[String], where: Seq[JDBCWhereClause], ordering: Seq[(String, Boolean)], limit: Boolean) extends JDBCPreparedQuery

sealed trait JDBCWhereClause

case class EQ(column: String) extends JDBCWhereClause

case class GT(column: String) extends JDBCWhereClause

case class GTE(column: String) extends JDBCWhereClause

case class LT(column: String) extends JDBCWhereClause

case class LTE(column: String) extends JDBCWhereClause

object JDBCPreparedQuery {


  def asSQL(q: JDBCPreparedQuery, mc: JDBCSQLConfig) = {
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
      case JDBCCreateTable(t, c, pk) =>
        val colStrings = c.map {
          case (cn, ct) => s"${mc.escapeColumnName(cn)} ${mc.sqlTypeToString(ct)}"
        }
        val withPK = colStrings :+ s"PRIMARY KEY${brackets(pk.map(mc.escapeColumnName))}"
        s"CREATE TABLE ${mc.escapeTableName(t)} ${brackets(withPK)}"
      case JDBCTruncate(t) => s"TRUNCATE TABLE ${mc.escapeTableName(t)}"
      case dt: JDBCDropTable => mc.dropTableSQL(dt)
    }
  }

  def exactMatch[T](s: Seq[ColumnMapping[JDBCColumn, T, _]]) = s.map(c => EQ(c.name))

}

