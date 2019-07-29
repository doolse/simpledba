package io.doolse.simpledba.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}

import cats.Monad
import io.doolse.simpledba.{ColumnRecord, ColumnRetrieve, JavaEffects, StreamEffects}
import shapeless.HList

trait WithJDBCConnection[S[-_, _], R]
{
  def apply[A](f: Connection => S[R, A]): S[R, A]
}

case class JDBCEffect[S[-_, _], F[-_, _], R](S: StreamEffects[S, F],
    inConnection: WithJDBCConnection[S, R],
    logger: JDBCLogger[F, R])(implicit JE: JavaEffects[F]) {

  def withLogger(log: JDBCLogger[F, R]): JDBCEffect[S, F, R] = copy(logger = log)

  def blockingIO[A](thunk: => A): F[Any, A] = JE.blockingIO(thunk)

  def logAndPrepare[PS](sql: String, f: Connection => PS): Connection => F[R, PS] =
    con => S.productR(logger.logPrepare(sql))(blockingIO(f(con)))

  def logAndBind[A](sql: String,
                    bindFunc: (Connection, PreparedStatement) => Seq[Any],
                    f: PreparedStatement => A): (Connection, PreparedStatement) => F[R, A] =
    (con, ps) =>
      S.flatMapF {
        blockingIO(bindFunc(con, ps))
      } { log =>
        S.productR(logger.logBind(sql, log))(blockingIO(f(ps)))
    }

  def executeStream[PS <: AutoCloseable, A](prepare: Connection => F[R, PS],
                                            bindAndExecute: (Connection, PS) => F[R, A]): S[R, A] = {
    inConnection { con =>
      S.flatMapS(S.bracket(prepare(con))(ps => blockingIO(ps.close()))) { ps =>
        S.eval(bindAndExecute(con, ps))
      }
    }
  }

  def executePreparedQuery(
      sql: String,
      bindFunc: (Connection, PreparedStatement) => Seq[Any]): S[R, (PreparedStatement, Boolean)] = {

    executeStream[PreparedStatement, (PreparedStatement, Boolean)](
      logAndPrepare(sql, _.prepareStatement(sql)),
      logAndBind(sql, bindFunc, ps => {
        (ps, ps.execute())
      })
    )
  }

  def executeResultSet(sql: String,
                       bindFunc: (Connection, PreparedStatement) => Seq[Any]): S[R, ResultSet] = {
    S.flatMapS(executePreparedQuery(sql, bindFunc)) {
      case (ps, _) =>
        S.read(blockingIO(ps.getResultSet))(rs => blockingIO(rs.close())) { rs: ResultSet =>
          S.mapF(blockingIO(rs.next()))(b => if (b) Some(rs) else None)
        }
    }
  }

  def resultSetRecord[C[A0] <: JDBCColumn[A0], Rec <: HList, A](
      cols: ColumnRecord[C, A, Rec],
      i: Int,
      rs: ResultSet
  ): F[R, Rec] = blockingIO {
    cols.mkRecord(new ColumnRetrieve[JDBCColumn, A] {
      override def apply[V](column: JDBCColumn[V], offset: Int, name: A): V =
        column.read(offset + i, rs) match {
          case None    => throw new Error(s"Column $name is null")
          case Some(v) => v.asInstanceOf[V]
        }
    })
  }

  def streamForQuery[C[A] <: JDBCColumn[A], Out <: HList](
      sql: String,
      bind: (Connection, PreparedStatement) => Seq[Any],
      resultCols: ColumnRecord[C, _, Out]): S[R, Out] = {
    S.evalMap(executeResultSet(sql, bind)) { rs =>
      resultSetRecord(resultCols, 1, rs)
    }
  }

  def flush(writes: S[R, JDBCWriteOp]): F[R, Unit] =
    S.drain(S.flatMapS(writes) {
      case JDBCWriteOp(sql, binder) => executePreparedQuery(sql, binder)
    })

}
