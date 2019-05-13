package io.doolse.simpledba

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import cats.data.{Kleisli, State, StateT}
import cats.effect.{IO, LiftIO, Sync}
import com.typesafe.config.{Config, ConfigFactory}
import fs2.{Pipe, Stream}
import cats.syntax.applicative._
import shapeless.{HList, HNil}

import scala.annotation.tailrec

package object jdbc {

  trait JDBCEffect[F[_]] {
    def acquire: F[Connection]
    def release: Connection => F[Unit]
    def blockingIO[A](thunk: => A): F[A]

    def executeStream[PS <: AutoCloseable, A](
        prepare: Connection => PS,
        bindAndExecute: (Connection, PS) => A): Stream[F, A] = {
      for {
        con <- Stream.bracket(acquire)(release)
        ps  <- Stream.bracket(blockingIO(prepare(con)))(ps => blockingIO(ps.close()))
        rs  <- Stream.eval(blockingIO(bindAndExecute(con, ps)))
      } yield rs
    }

    def nextLoop(rs: ResultSet): Stream[F, ResultSet] =
      Stream.eval(blockingIO(rs.next())).flatMap { n =>
        if (n) Stream(rs) ++ nextLoop(rs) else Stream.empty
      }

    def executePreparedQuery(
        psql: JDBCPreparedQuery,
        config: JDBCConfig,
        bindFunc: BindFunc[Seq[BindLog]]): Stream[F, (PreparedStatement, Boolean)] = {
      val sql = config.queryToSQL(psql)

      executeStream[PreparedStatement, (PreparedStatement, Boolean)](
        con => {
          config.logPrepare(sql)
          con.prepareStatement(sql)
        },
        (con, ps) => {
          val log = bindFunc(con, ps).runA(1).value
          config.logBind(sql, log)
          (ps, ps.execute())
        }
      )
    }

    def executeResultSet(psql: JDBCPreparedQuery,
                         config: JDBCConfig,
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
        config: JDBCConfig,
        query: JDBCPreparedQuery,
        bind: BindFunc[Seq[BindLog]],
        resultCols: ColumnRecord[C, _, Out]
    ): Stream[F, Out] = {
      executeResultSet(query, config, bind).evalMap { rs =>
        resultSetRecord(resultCols, 1, rs)
      }
    }

  }

  type JDBCIO2[A] = StateT[IO, Connection, A]

  type BindFunc[A] = Kleisli[State[Int, ?], (Connection, PreparedStatement), A]

  type ParamBinder = (Int, Connection, PreparedStatement) => Unit

  implicit val oneConnection: JDBCEffect[JDBCIO2] = new JDBCEffect[JDBCIO2] {
    override def acquire: JDBCIO2[Connection] = StateT.get

    override def release: Connection => JDBCIO2[Unit] = _ => ().pure[JDBCIO2]

    override def blockingIO[A](thunk: => A): JDBCIO2[A] = StateT.liftF(IO.delay(thunk))
  }

  implicit val flushJDBCIO = flushJDBC[JDBCIO2]

  def flushJDBC[F[_]](implicit C: JDBCEffect[F], F: Sync[F]): Flushable[F] =
    new Flushable[F] {
      def flush: Pipe[F, WriteOp, Unit] =
        s =>
          Stream.eval(
            s.flatMap {
                case JDBCWriteOp(q, config, binder) =>
                  C.executePreparedQuery(q, config, binder)
              }
              .compile
              .drain)
    }

  def connectionFromConfig(config: Config = ConfigFactory.load()): Connection = {
    val jdbcConfig = config.getConfig("simpledba.jdbc")
    val jdbcUrl    = jdbcConfig.getString("url")
    if (jdbcConfig.hasPath("credentials")) {
      val cc = jdbcConfig.getConfig("credentials")
      DriverManager.getConnection(jdbcUrl, cc.getString("username"), cc.getString("password"))
    } else DriverManager.getConnection(jdbcUrl)
  }

  def rawSQL(sql: String)(implicit config: JDBCConfig): WriteOp = {
    JDBCWriteOp(JDBCRawSQL(sql), config, Kleisli.pure(Seq.empty))
  }

  def rawSQLStream[F[_]](
      sql: Stream[F, String]
  )(implicit config: JDBCConfig): Stream[F, WriteOp] = {
    sql.map(rawSQL)
  }
}
