package io.doolse.simpledba.cassandra

import java.util.concurrent.ExecutionException

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, DowngradingConsistencyRetryPolicy, LoggingRetryPolicy, TokenAwarePolicy}
import com.google.common.util.concurrent.ListenableFuture
import fs2.Strategy
import fs2.util.Task
import io.doolse.simpledba.ColumnName

import scala.concurrent.ExecutionContext.Implicits
import scala.util.{Failure, Success, Try}

/**
  * Created by jolz on 5/05/16.
  */
object CassandraSession {

  implicit val strat = Strategy.fromExecutionContext(Implicits.global)

  def simpleSession(hosts: String, ks: Option[String]) = {
    val cluster = Cluster.builder()
      .addContactPoints(hosts)
      .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy.Builder().build()))
      .withRetryPolicy(new LoggingRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE))
      .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM)).build()
    ks.map(cluster.connect).getOrElse(cluster.connect)
  }

  def executeLater(stmt: Statement, session: Session) = Task.suspend(executeAsync(stmt, session))
  def executeAsync(stmt: Statement, session: Session) = async(session.executeAsync(stmt), stmt.toString)

  def async[A](lf: ListenableFuture[A], stmt: => String) = Task.async[A] { k =>
    lf.addListener(new Runnable {
      override def run(): Unit = k {
        Try(lf.get()) match {
          case Success(a) ⇒ Right(a)
          case Failure(ee: ExecutionException) ⇒ Left(new CassandraIOException(s"Failed executing - $stmt", ee.getCause))
          case Failure(x) ⇒ Left(x)
        }
      }
    }, Implicits.global)
  }

  class CassandraIOException(msg: String, t: Throwable) extends RuntimeException(msg, t)

  val reservedColumns = Set("schema")
  def escapeColumn(name: ColumnName): String =
    escapeReserved(name.name)

  def escapeReserved(name: String) =
    if (reservedColumns(name.toLowerCase())) '"'+name+'"' else name


}
