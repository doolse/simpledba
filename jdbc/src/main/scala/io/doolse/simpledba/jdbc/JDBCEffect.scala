package io.doolse.simpledba.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}

import cats.Monad
import cats.data.{Kleisli, State}
import fs2.Stream
import io.doolse.simpledba.{ColumnRecord, Flushable}
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
      psql: JDBCPreparedQuery,
      config: SQLDialect,
      bindFunc: BindFunc[Seq[BindLog]]): Stream[F, (PreparedStatement, Boolean)] = {

    val sql = config.querySQL(psql)

    executeStream[PreparedStatement, (PreparedStatement, Boolean)](
      logAndPrepare(sql, _.prepareStatement(sql)),
      logAndBind(sql, bindFunc, ps => { (ps, ps.execute()) })
    )
  }

  def executeResultSet(psql: JDBCPreparedQuery,
                       config: SQLDialect,
                       bindFunc: BindFunc[Seq[BindLog]]): Stream[F, ResultSet] = {
    executePreparedQuery(psql, config, bindFunc)
      .flatMap {
        case (ps, _) => Stream.bracket(blockingIO(ps.getResultSet))(rs => blockingIO(rs.close()))
      }
      .flatMap(nextLoop)
  }

  def resultSetRecord[C[_] <: JDBCColumn, R <: HList](
      cols: ColumnRecord[C, _, R],
      i: Int,
      rs: ResultSet
  ): F[R] = blockingIO {
    @tailrec
    def loop(offs: Int, l: HList): HList = {
      if (offs < 0) l
      else {
        val (name, col) = cols.columns(offs)
        col.read(offs + i, rs) match {
          case None    => throw new Error(s"Column $name is null")
          case Some(v) => loop(offs - 1, v :: l)
        }
      }
    }

    loop(cols.columns.length - 1, HNil).asInstanceOf[R]
  }

  def streamForQuery[C[_] <: JDBCColumn, Out <: HList](
      config: SQLDialect,
      query: JDBCPreparedQuery,
      bind: BindFunc[Seq[BindLog]],
      resultCols: ColumnRecord[C, _, Out]
  ): Stream[F, Out] = {
    executeResultSet(query, config, bind).evalMap { rs =>
      resultSetRecord(resultCols, 1, rs)
    }
  }

}
