package io.doolse.simpledba.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}

import cats.Monad
import fs2.Stream
import io.doolse.simpledba.{ColumnRecord, ColumnRetrieve, Flushable}
import shapeless.{HList, HNil}

import scala.annotation.tailrec

trait JDBCEffect[F[_]] {

  def acquire: F[Connection]
  def release: Connection => F[Unit]
  def blockingIO[A](thunk: => A): F[A]
  def M: Monad[F]
  def logger: JDBCLogger[F]
  def flushable: Flushable[F]

  def logAndPrepare[PS](sql: String, f: Connection => PS): Connection => F[PS] =
    con => M.productR(logger.logPrepare(sql))(blockingIO(f(con)))

  def logAndBind[A](sql: String,
                    bindFunc: BindFunc[Seq[BindLog]],
                    f: PreparedStatement => A): (Connection, PreparedStatement) => F[A] =
    (con, ps) =>
      M.flatMap { blockingIO(bindFunc.apply(con, ps).runA(1).value) } { log =>
        M.productR(logger.logBind(sql, log))(blockingIO(f(ps)))
    }

  def executeStream[PS <: AutoCloseable, A](
      prepare: Connection => F[PS],
      bindAndExecute: (Connection, PS) => F[A]): Stream[F, A] = {
    for {
      con <- Stream.bracket(acquire)(release)
      ps  <- Stream.bracket(prepare(con))(ps => blockingIO(ps.close()))
      rs  <- Stream.eval(bindAndExecute(con, ps))
    } yield rs
  }

  def nextLoop(rs: ResultSet): Stream[F, ResultSet] =
    Stream.eval(blockingIO(rs.next())).flatMap { n =>
      if (n) Stream(rs) ++ nextLoop(rs) else Stream.empty
    }

  def executePreparedQuery(
      sql: String,
      bindFunc: BindFunc[Seq[BindLog]]): Stream[F, (PreparedStatement, Boolean)] = {

    executeStream[PreparedStatement, (PreparedStatement, Boolean)](
      logAndPrepare(sql, _.prepareStatement(sql)),
      logAndBind(sql, bindFunc, ps => { (ps, ps.execute()) })
    )
  }

  def executeResultSet(sql: String,
                       bindFunc: BindFunc[Seq[BindLog]]): Stream[F, ResultSet] = {
    executePreparedQuery(sql, bindFunc)
      .flatMap {
        case (ps, _) => Stream.bracket(blockingIO(ps.getResultSet))(rs => blockingIO(rs.close()))
      }
      .flatMap(nextLoop)
  }

  def resultSetRecord[C[_] <: JDBCColumn, R <: HList, A](
      cols: ColumnRecord[C, A, R],
      i: Int,
      rs: ResultSet
  ): F[R] = blockingIO {
    cols.mkRecord(new ColumnRetrieve[C, A] {
      override def apply[V](column: C[V], offset: Int, name: A): V =
        column.read(offset + i, rs) match {
          case None    => throw new Error(s"Column $name is null")
          case Some(v) => v.asInstanceOf[V]
        }
    })
  }

  def streamForQuery[C[_] <: JDBCColumn, Out <: HList](
      sql: String,
      bind: BindFunc[Seq[BindLog]],
      resultCols: ColumnRecord[C, _, Out]
  ): Stream[F, Out] = {
    executeResultSet(sql, bind).evalMap { rs =>
      resultSetRecord(resultCols, 1, rs)
    }
  }

}
