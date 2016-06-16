package io.doolse.simpledba.cassandra

import java.util.concurrent.ExecutionException

import cats.data.Kleisli
import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, DowngradingConsistencyRetryPolicy, LoggingRetryPolicy, TokenAwarePolicy}
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

  def executeLater(stmt: Statement, session: Session) = Task.suspend(executeAsync(stmt, session))
  def executeAsync(stmt: Statement, session: Session) = asyncStmt(session.executeAsync(stmt), stmt.toString)

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
    if (reservedColumns(name.toLowerCase())) '"'+name+'"' else name

}

case class SessionConfig(session: Session, logger: (() ⇒ String) ⇒ Unit = _ => ()) {
  def executeLater(stmt: Statement): Task[ResultSet] = {
    logger(() => stmt.toString())
    CassandraSession.executeLater(stmt, session)
  }
}
