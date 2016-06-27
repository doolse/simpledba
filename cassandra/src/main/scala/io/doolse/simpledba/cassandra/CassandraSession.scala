package io.doolse.simpledba.cassandra

import java.util.concurrent.{ConcurrentHashMap, ExecutionException}

import cats.data.Kleisli
import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, DowngradingConsistencyRetryPolicy, LoggingRetryPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder._
import com.datastax.driver.core.querybuilder.Select.{Selection, SelectionOrAlias, Where}
import com.google.common.util.concurrent.ListenableFuture
import com.typesafe.config.{Config, ConfigFactory}
import fs2.{Chunk, Strategy, Stream}
import fs2.util.{Task, ~>}
import io.doolse.simpledba.cassandra.CassandraMapper.Effect

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

/**
  * Created by jolz on 5/05/16.
  */
object CassandraSession {

  implicit val strat = Strategy.fromExecutionContext(ExecutionContext.global)

  def initSimpleSession(config: Config = ConfigFactory.load()) = {
    val cassConfig = config.getConfig("simpledba.cassandra")
    val hosts = cassConfig.getStringList("hosts").asScala
    val ks = if (cassConfig.hasPath("keyspace")) Some(cassConfig.getString("keyspace")) else None
    simpleSession(hosts.mkString(","), ks)
  }

  def simpleSession(hosts: String, ks: Option[String] = None) = {
    val cluster = Cluster.builder()
      .addContactPoints(hosts)
      .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy.Builder().build()))
      .withRetryPolicy(new LoggingRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE))
      .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM)).build()
    ks.map(cluster.connect).getOrElse(cluster.connect)
  }

  def stmt2String(s: Statement): String = s match {
    case bs : BoundStatement => bs.preparedStatement.getQueryString
    case rs : RegularStatement => rs.getQueryString
    case s => s.toString()
  }

  def executeLater(stmt: Statement, session: Session) = Task.suspend(executeAsync(stmt, session))

  def executeAsync(stmt: Statement, session: Session) = asyncStmt(session.executeAsync(stmt), stmt2String(stmt))

  def async[A](lf: ListenableFuture[A], fromEE: ExecutionException => Throwable) = Task.async[A] { k =>
    lf.addListener(new Runnable {
      override def run(): Unit = k {
        Try(lf.get()) match {
          case Success(a) ⇒ Right(a)
          case Failure(ee: ExecutionException) ⇒ Left(fromEE(ee))
          case Failure(x) ⇒ Left(x)
        }
      }
    }, Implicits.global)
  }

  def asyncStmt[A](lf: ListenableFuture[A], stmt: => String) =
    async[A](lf, ee => new CassandraIOException(s"Failed executing - $stmt - cause ${ee.getMessage}", ee.getCause))

  val effect2Task = new (Task ~> Effect) {
    def apply[A](f: Task[A]): Effect[A] = Kleisli(_ => f)
  }

  def rowsStream(rs: ResultSet)(implicit strat: fs2.Strategy): Stream[Task, Row] = {
    if (rs.isExhausted) Stream.empty[Task, Row]
    else {
      val left = rs.getAvailableWithoutFetching
      if (left > 0) {
        val buf = mutable.Buffer[Row]()
        Range(0, left).foreach(_ => buf += rs.one)
        Stream.chunk[Task, Row](Chunk.seq(buf)) ++ rowsStream(rs)
      } else {
        Stream.eval(async(rs.fetchMoreResults(), identity)).flatMap(rowsStream(_))
      }
    }
  }

  class CassandraIOException(msg: String, t: Throwable) extends RuntimeException(msg, t)

  val reservedColumns = Set("schema")

  def escapeReserved(name: String) =
    if (reservedColumns(name.toLowerCase())) '"' + name + '"' else name

}

sealed trait PreparableStatement {
  def build: RegularStatement
}

case class SessionConfig(session: Session, logger: (() ⇒ String) ⇒ Unit = _ => ()) {
  val statementCache = new ConcurrentHashMap[Any, Task[PreparedStatement]]().asScala

  def executeLater(stmt: Statement): Task[ResultSet] = {
    logger(() => CassandraSession.stmt2String(stmt))
    CassandraSession.executeLater(stmt, session)
  }

  def prepareAndBind[A <: PreparableStatement](a: A, b: Seq[AnyRef]): Task[ResultSet] = {
    lazy val prepared = {
      val built = a.build
      logger(() => "Preparing: "+CassandraSession.stmt2String(built))
      CassandraSession.asyncStmt(session.prepareAsync(built), built.getQueryString)
    }
    statementCache.getOrElseUpdate(a, prepared).flatMap {
      ps =>
        logger(() => "Binding "+b.mkString(", ")+" to "+ps.getQueryString)
        CassandraSession.executeAsync(ps.bind(b: _*), session)
    }
  }
}

sealed trait CassandraAssignment {
  def toAssignment(v: Any): Assignment
}

case class SetAssignment(column: String) extends CassandraAssignment {
  def toAssignment(v: Any): Assignment = QueryBuilder.set(column,v)
}

sealed trait CassandraClause {
  def toClause(v: AnyRef): Clause
}

case class CassandraGT(name: String) extends CassandraClause {
  def toClause(v: AnyRef) = QueryBuilder.gt(CassandraSession.escapeReserved(name), v)
}

case class CassandraGTE(name: String) extends CassandraClause {
  def toClause(v: AnyRef) = QueryBuilder.gte(CassandraSession.escapeReserved(name), v)
}

case class CassandraLTE(name: String) extends CassandraClause {
  def toClause(v: AnyRef) = QueryBuilder.lte(CassandraSession.escapeReserved(name), v)
}

case class CassandraLT(name: String) extends CassandraClause {
  def toClause(v: AnyRef) = QueryBuilder.lt(CassandraSession.escapeReserved(name), v)
}

case class CassandraEQ(name: String) extends CassandraClause {
  def toClause(v: AnyRef) = QueryBuilder.eq(CassandraSession.escapeReserved(name), v)
}

case class CassandraSelect(table: String, columns: Seq[String], where: Seq[CassandraClause], ordering: Seq[(String, Boolean)], limit: Boolean) extends PreparableStatement {
  def build: RegularStatement = {
    val sel = QueryBuilder.select()
    columns.foreach(sel.column)
    val s = sel.from(table)
    val orderings = ordering.map { case (c, asc) =>
      val esc = CassandraSession.escapeReserved(c)
      if (asc) QueryBuilder.asc(esc) else QueryBuilder.desc(esc)
    }
    val marker = QueryBuilder.bindMarker()
    where.foreach(c => s.where(c.toClause(marker)))
    if (limit) s.limit(marker)
    if (orderings.nonEmpty) s.orderBy(orderings : _*)
    s
  }
}

case class CassandraInsert(table: String, columns: Seq[String]) extends PreparableStatement {
  def build = {
    val ins = QueryBuilder.insertInto(table)
    columns.foreach(c => ins.value(CassandraSession.escapeReserved(c), QueryBuilder.bindMarker()))
    ins
  }
}

case class CassandraUpdate(table: String, assignments: Seq[CassandraAssignment], where: Seq[CassandraClause]) extends PreparableStatement {
  def build = {
    val upd = QueryBuilder.update(table)
    val updateWith = upd.`with`
    val updateWhere = upd.where()
    assignments.foreach { asgn => updateWith.and(asgn.toAssignment(QueryBuilder.bindMarker())) }
    where.foreach(c => updateWhere.and(c.toClause(QueryBuilder.bindMarker())))
    upd
  }
}

case class CassandraDelete(table: String, where: Seq[CassandraClause]) extends PreparableStatement {
  def addClause(c: CassandraClause, w: Delete.Where) = w.and(c.toClause(QueryBuilder.bindMarker()))
  def build = {
    val del = QueryBuilder.delete().all().from(table)
    val delWhere = del.where()
    where.foreach(c => delWhere.and(c.toClause(QueryBuilder.bindMarker())))
    del
  }
}