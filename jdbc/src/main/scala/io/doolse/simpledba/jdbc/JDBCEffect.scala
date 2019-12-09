package io.doolse.simpledba.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}

import cats.Monad
import io.doolse.simpledba.{ColumnRecord, ColumnRetrieve, JavaEffects, Streamable}
import shapeless.HList

trait WithJDBCConnection[S[_]] {
  def apply[A](f: Connection => S[A]): S[A]
}

class JDBCEffect[S[_], F[_]](inConnection: WithJDBCConnection[S],
                                          logger: JDBCLogger[F])(implicit JE: JavaEffects[F], val S: Streamable[S, F], val M: Monad[F]) {

  def withLogger(l: JDBCLogger[F]): JDBCEffect[S, F] = new JDBCEffect(inConnection, l)

  def blockingIO[A](thunk: => A): F[A] = JE.blockingIO(thunk)

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

  def executeStream[PS <: AutoCloseable, A](
      prepare: Connection => F[PS],
      bindAndExecute: (Connection, PS) => F[A]): S[A] = {
    inConnection { con =>
      S.flatMapS(S.bracket(prepare(con))(ps => blockingIO(ps.close()))) { ps =>
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
    S.flatMapS(executePreparedQuery(sql, bindFunc)) {
      case (ps, _) =>
        S.read(blockingIO(ps.getResultSet))(rs => blockingIO(rs.close())) { rs: ResultSet =>
          M.map(blockingIO(rs.next()))(b => if (b) Some(rs) else None)
        }
    }
  }

  def resultSetRecord[C[A0] <: JDBCColumn[A0], Rec <: HList, A](
      cols: ColumnRecord[C, A, Rec],
      i: Int,
      rs: ResultSet
  ): F[Rec] = blockingIO {
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
    S.drain(S.flatMapS(writes) {
      case JDBCWriteOp(sql, binder) => executePreparedQuery(sql, binder)
    })

}

object JDBCEffect {
  def apply[S[_], F[_]: Monad](
            inConnection: WithJDBCConnection[S])(implicit JE: JavaEffects[F], S: Streamable[S, F]): JDBCEffect[S, F] = {
    new JDBCEffect[S, F](inConnection, NothingLogger())
  }

  def withLogger[S[_], F[_]: Monad](
      inConnection: WithJDBCConnection[S],
      logger: JDBCLogger[F])(implicit JE: JavaEffects[F], S: Streamable[S, F]): JDBCEffect[S, F] = {
    new JDBCEffect[S, F](inConnection, logger)
  }
}
