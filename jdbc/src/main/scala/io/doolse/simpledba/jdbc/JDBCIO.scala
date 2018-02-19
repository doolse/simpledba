package io.doolse.simpledba.jdbc

import java.sql._
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import io.doolse.simpledba.{ColumnMapping, ColumnMaterialzer, PhysicalValue, WriteOp}
import fs2._
import JDBCUtils.brackets
import cats.data.{Kleisli, ReaderT, StateT, WriterT}
import cats.effect.IO
import com.typesafe.config.{Config, ConfigFactory}
import cats.instances.vector._
import scala.concurrent.ExecutionContext.Implicits.global
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
      case "sqlserver" => JDBCSQLConfig.sqlServerConfig
      case "oracle" => JDBCSQLConfig.oracleConfig
    }
    val con = if (jdbcConfig.hasPath("credentials")) {
      val cc = jdbcConfig.getConfig("credentials")
      DriverManager.getConnection(jdbcUrl, cc.getString("username"), cc.getString("password"))
    } else DriverManager.getConnection(jdbcUrl)
    JDBCSession(con, dialect, logger = logger.getOrElse(_ => ()))
  }

  def pureJDBC[A](a: A) : Effect[A] = StateT.pure(a)
  def liftJDBC[A](a: IO[A]): Effect[A] = StateT.liftF(a)

  def rowsStream[A](open: Effect[ResultSet]): Stream[Effect, ResultSet] = {
    def nextLoop(rs: ResultSet): Stream[Effect, ResultSet] = {
      Stream.eval(liftJDBC(IO(rs.next()))).flatMap(n => if (n) Stream(rs) ++ nextLoop(rs) else Stream.empty)
    }
    Stream.eval[Effect, JDBCSession](StateT.get)
      .flatMap(s => Stream.bracket(open)(nextLoop, rs => liftJDBC(IO(rs.close()))))
  }

  def rowMaterializer(c: JDBCSession, r: ResultSet) = new ColumnMaterialzer[JDBCColumn] {
    def apply[A](name: String, atom: JDBCColumn[A]): A = atom.byName(c, r, name).getOrElse(sys.error(s"Mapping error - column '$name' contains a NULL"))
  }

  def sessionIO[A](f: JDBCSession => IO[A]): Effect[A] =
    StateT.inspectF(f)

  def write(f: JDBCWrite): Stream[Effect, WriteOp] = Stream.emit(f)

  def writeSink: Sink[Effect, WriteOp] = { st =>
    st.segmentN(1000).evalMap { chunk =>
      val batches = chunk.map {
        case JDBCWrite(q, v) => (q, v)
      }.force.toVector.groupBy(_._1).toSeq
      StateT.inspectF { (s:JDBCSession) =>
        val alls: Stream[IO, Stream[IO, Unit]] = Stream.emits {
          batches.map {
            b => Stream.eval(s.execBatch(b._1, b._2.map(_._2)))
          }
        }
        alls.join(4).compile.drain
      }
    }
  }
}

case class JDBCSession(connection: Connection, config: JDBCSQLConfig, logger: (() ⇒ String) ⇒ Unit = _ => (),
                       statementCache: scala.collection.concurrent.Map[Any, IO[PreparedStatement]]
                       = new ConcurrentHashMap[Any, IO[PreparedStatement]]().asScala) {

  def prepare(q: JDBCPreparedQuery): IO[PreparedStatement] = {
    lazy val prepared = {
      logger(() => "Preparing: " + JDBCPreparedQuery.asSQL(q, config))
      IO.pure(connection.prepareStatement(JDBCPreparedQuery.asSQL(q, config)))
    }
    statementCache.getOrElseUpdate(q, prepared)
  }

  def doBind(ps: PreparedStatement, s: Seq[PhysicalValue[JDBCColumn]]): PreparedStatement = {
    s.zipWithIndex.foreach {
      case (pv, i) => pv.atom.bind(this, ps, i + 1, pv.v)
    }
    ps
  }

  def prepareAndBind(q: JDBCPreparedQuery, s: Seq[PhysicalValue[JDBCColumn]]): IO[PreparedStatement] = {
    prepare(q).map { ps =>
      logger {
        () =>
          val vals = s.map(pv => pv.name + "=" + pv.v).mkString(",")
          "Statement: " + JDBCPreparedQuery.asSQL(q, config) + " with values " + vals
      }
      doBind(ps, s)
    }
  }

  def execQuery(q: JDBCPreparedQuery, s: Seq[PhysicalValue[JDBCColumn]]): IO[ResultSet] = prepareAndBind(q, s).map(_.executeQuery())

  def execWrite(q: JDBCPreparedQuery, s: Seq[PhysicalValue[JDBCColumn]]): IO[Boolean] = prepareAndBind(q, s).map(_.execute())

  def execBatch(q: JDBCPreparedQuery, bvals: Seq[Seq[PhysicalValue[JDBCColumn]]]): IO[Unit] = prepare(q).map { ps =>
    logger(() => s"Batch statement: ${JDBCPreparedQuery.asSQL(q, config)}")
    bvals.foreach { s =>
      logger { () => s"Values ${s.map(pv => pv.name + "=" + pv.v).mkString(",")}" }
      doBind(ps, s).addBatch()
    }
    ps.executeBatch()
    ()
  }
}

case class JDBCWrite(q: JDBCPreparedQuery, s: Seq[PhysicalValue[JDBCColumn]]) extends WriteOp

sealed trait JDBCPreparedQuery

case class JDBCDropTable(name: String) extends JDBCPreparedQuery

case class JDBCCreateTable(name: String, columns: Seq[(String, SQLType)], primaryKey: Seq[String]) extends JDBCPreparedQuery

case class JDBCTruncate(name: String) extends JDBCPreparedQuery

case class JDBCInsert(table: String, columns: Seq[String]) extends JDBCPreparedQuery

case class JDBCUpdate(table: String, assignments: Seq[String], where: Seq[JDBCWhereClause]) extends JDBCPreparedQuery

case class JDBCDelete(table: String, where: Seq[JDBCWhereClause]) extends JDBCPreparedQuery

case class JDBCSelect(table: String, columns: Seq[String], where: Seq[JDBCWhereClause], ordering: Seq[(String, Boolean)], limit: Boolean) extends JDBCPreparedQuery

case class JDBCRawSQL(sql: String) extends JDBCPreparedQuery

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
      case JDBCRawSQL(sql) => sql
    }
  }

  def exactMatch[T](s: Seq[ColumnMapping[JDBCColumn, T, _]]) = s.map(c => EQ(c.name))

}

