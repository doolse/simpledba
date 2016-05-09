package io.doolse.simpledba.cassandra

import java.util.concurrent.ConcurrentHashMap

import cats.{Monad, MonadState}
import cats.data.{ReaderT, State}
import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.{Clause, QueryBuilder}
import io.doolse.simpledba._
import CassandraSession._
import fs2.util.Task
import io.doolse.simpledba.cassandra.CassandraRelationIO.{Effect, ResultSetT}

import scala.collection.JavaConverters._

/**
  * Created by jolz on 5/05/16.
  */

sealed trait CassandraColumn[T] {
  def byName(r: Row, n: String): Option[T]

  def byIndex(r: Row, idx: Int): Option[T]

  val binding: T => AnyRef
}

object CassandraCodecColumn {
  def direct[T](tc: TypeCodec[T]): CassandraColumn[T] = CassandraCodecColumn[T, T](tc, identity, _.asInstanceOf[AnyRef])
}

case class CassandraCodecColumn[T0, T](tt: TypeCodec[T0], from: T0 => T, binding: T => AnyRef) extends CassandraColumn[T] {
  def byName(r: Row, n: String): Option[T] = Option(r.get(n, tt)).map(from)

  def byIndex(r: Row, idx: Int): Option[T] = Option(r.get(idx, tt)).map(from)
}

case class CassandraResultSet(rs: ResultSet, row: Option[Row])

sealed trait SessionConfig {
  def session: Session

  def prepareQuery(query: RelationQuery): Task[PreparedStatement]

  def logger: (() ⇒ String) ⇒ Unit
}

object CassandraRelationIO {

  type Effect[A] = ReaderT[Task, SessionConfig, A]
  type ResultSetT[A] = State[CassandraResultSet, A]

  def apply() = new CassandraRelationIO

  val doNothingLogger = (_: () ⇒ String) ⇒ ()

  def initialiseSession(_session: Session, queryLogger: (() ⇒ String) ⇒ Unit = doNothingLogger) = new SessionConfig {
    override def session: Session = _session

    val logger = queryLogger

    val marker = QueryBuilder.bindMarker()

    val queryCache = new ConcurrentHashMap[RelationQuery, Task[PreparedStatement]]().asScala

    def addWhere[A](f: Clause ⇒ A)(n: ColumnName) = f(QueryBuilder.eq(escapeColumn(n), marker))

    override def prepareQuery(query: RelationQuery): Task[PreparedStatement] = queryCache.getOrElseUpdate(query, {
      val stmt = query match {
        case SelectQuery(table, cols, pks, orderBy) ⇒
          val selectQ = QueryBuilder.select(cols.map(escapeColumn): _*).from(escapeReserved(table))
          orderBy.foreach(o ⇒ selectQ.orderBy((if (o.ascending) QueryBuilder.asc _ else QueryBuilder.desc _).apply(o.name.name)))
          pks.foreach(addWhere(selectQ.where))
          selectQ
        case DeleteQuery(table, pks) ⇒
          val deleteQ = QueryBuilder.delete().all().from(escapeReserved(table))
          pks.foreach(addWhere(deleteQ.where))
          deleteQ
        case InsertQuery(table, cols) ⇒
          val insertQ = QueryBuilder.insertInto(escapeReserved(table))
          cols.foreach(n ⇒ insertQ.value(escapeColumn(n), marker))
          insertQ
        case UpdateQuery(table, modCols, pks) ⇒
          val updateQ = QueryBuilder.update(escapeReserved(table))
          modCols.foreach(n ⇒ updateQ.`with`(QueryBuilder.set(escapeColumn(n), marker)))
          pks.foreach(addWhere(updateQ.where))
          updateQ
      }
      logger(() => s"Preparing ${stmt.getQueryString()}")
      async(session.prepareAsync(stmt), stmt.getQueryString())
    })
  }

}

class CassandraRelationIO extends RelationIO[Effect, ResultSetT] {
  type CT[T] = CassandraColumn[T]

  type RS = CassandraResultSet

  val resultSetOperations = new ResultSetOps[ResultSetT, CassandraColumn] {

    val MS = MonadState[ResultSetT, CassandraResultSet]

    import MS._

    def isNull(ref: ColumnReference) = inspect {
      _.row.map { row =>
        ref match {
          case ColumnIndex(i) => row.isNull(i)
          case ColumnName(n) => row.isNull(n)
        }
      } getOrElse sys.error("isNull called with no result")
    }

    def haveMoreResults = inspect(_.rs.isExhausted)

    def nextResult = State { (s: CassandraResultSet) =>
      val news = s.copy(row = Option(s.rs.one()))
      (news, news.row.isDefined)
    }

    def getColumn[T](ref: ColumnReference, ct: CassandraColumn[T]): State[CassandraResultSet, Option[T]] = inspect {
      _.row.map { row =>
        ref match {
          case ColumnName(name) => ct.byName(row, name)
          case ColumnIndex(idx) => ct.byIndex(row, idx)
        }
      } getOrElse sys.error("getColumn called on empty row")
    }
  }

  def usingResults[A](rs: CassandraResultSet, op: ResultSetT[A]) = ReaderT(_ => Task.now(op.runA(rs).value))

  def valueFromQP[A](qp: QP[A]): AnyRef = qp match {
    case Some((v, colT)) => colT.binding(v)
    case None => null
  }

  def query(q: RelationQuery, params: Iterable[QP[Any]]) = ReaderT { sc =>
    for {
      ps <- sc.prepareQuery(q)
      binds = params.toSeq.map(valueFromQP)
      rs <- CassandraSession.executeLater(ps.bind(binds: _*), sc.session)
    } yield CassandraResultSet(rs, None)
  }

  implicit val M = Monad[ResultSetT]
}
