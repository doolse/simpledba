package io.doolse.simpledba.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}

import cats.Monad
import io.doolse.simpledba.{ColumnRecord, ColumnRetrieve, JavaEffects, Streamable}
import shapeless.HList

trait WithJDBCConnection[S[_], F[_]]
{
  def apply[A](f: Connection => S[A]): S[A]
}

case class SingleJDBCConnection[S[_], F[_]](con: Connection) extends WithJDBCConnection[S, F]
{
  override def apply[A](f: Connection => S[A]): S[A] = f(con)
}

case class JDBCEffect[S[_], F[_]](
    inConnection: WithJDBCConnection[S, F],
    logger: JDBCLogger[F])(implicit val S: Streamable[S, F], M: Monad[F], JE: JavaEffects[F]) {

  def withLogger(log: JDBCLogger[F]): JDBCEffect[S, F] = copy(logger = log)

  def blockingIO[A](thunk: => A): F[A] = JE.blockingIO(thunk)

  private def SM = S.SM

  def logAndPrepare[PS](sql: String, f: Connection => PS): Connection => F[PS] =
    con => M.productR(logger.logPrepare(sql))(blockingIO(f(con)))

  def logAndBind[A](sql: String,
                    bindFunc: (Connection, PreparedStatement) => Seq[Any],
                    f: PreparedStatement => A): (Connection, PreparedStatement) => F[A] =
    (con, ps) =>
      M.flatMap {
        blockingIO(bindFunc(con, ps))
      } { log =>
        M.productR(logger.logBind(sql, log))(blockingIO(f(ps)))
    }

  def executeStream[PS <: AutoCloseable, A](prepare: Connection => F[PS],
                                            bindAndExecute: (Connection, PS) => F[A]): S[A] = {
    inConnection { con =>
      SM.flatMap(S.bracket(prepare(con))(ps => blockingIO(ps.close()))) { ps =>
        S.eval(bindAndExecute(con, ps))
      }
    }
  }

  def executePreparedQuery(
      sql: String,
      bindFunc: (Connection, PreparedStatement) => Seq[Any]): S[(PreparedStatement, Boolean)] = {

    executeStream[PreparedStatement, (PreparedStatement, Boolean)](
      logAndPrepare(sql, _.prepareStatement(sql)),
      logAndBind(sql, bindFunc, ps => {
        (ps, ps.execute())
      })
    )
  }

  def executeResultSet(sql: String,
                       bindFunc: (Connection, PreparedStatement) => Seq[Any]): S[ResultSet] = {
    SM.flatMap(executePreparedQuery(sql, bindFunc)) {
      case (ps, _) =>
        S.read(blockingIO(ps.getResultSet))(rs => blockingIO(rs.close())) { rs: ResultSet =>
          M.map(blockingIO(rs.next()))(b => if (b) Some(rs) else None)
        }
    }
  }

  def resultSetRecord[C[A0] <: JDBCColumn[A0], R <: HList, A](
      cols: ColumnRecord[C, A, R],
      i: Int,
      rs: ResultSet
  ): F[R] = blockingIO {
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
      resultCols: ColumnRecord[C, _, Out]): S[Out] = {
    S.evalMap(executeResultSet(sql, bind)) { rs =>
      resultSetRecord(resultCols, 1, rs)
    }
  }

  def flush(writes: S[JDBCWriteOp]): F[Unit] =
    S.drain(SM.flatMap(writes) {
      case JDBCWriteOp(sql, binder) => executePreparedQuery(sql, binder)
    })

}
