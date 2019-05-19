package io.doolse.simpledba

import java.sql.{Connection, DriverManager, PreparedStatement}

import cats.Monad
import cats.data.{Kleisli, State, StateT}
import cats.effect.{IO, Sync}
import cats.syntax.applicative._
import com.typesafe.config.{Config, ConfigFactory}
import fs2.{Pipe, Stream}

package object jdbc {

  type BindFunc[A] = Kleisli[State[Int, ?], (Connection, PreparedStatement), A]

  type ParamBinder = (Int, Connection, PreparedStatement) => Unit

  type JDBCIO[A] = StateT[IO, Connection, A]

  case class StateIOEffect(logger: JDBCLogger[JDBCIO] = new NothingLogger)(
      implicit val M: Monad[JDBCIO])
      extends JDBCEffect[JDBCIO] {
    override def acquire: JDBCIO[Connection] = StateT.get

    override def release: Connection => JDBCIO[Unit] = _ => ().pure[JDBCIO]

    override def blockingIO[A](thunk: => A): JDBCIO[A] = StateT.liftF(IO.delay(thunk))

    override def flushable: Flushable[JDBCIO] = flushJDBC(this)
  }

  def flushJDBC[F[_]](C: JDBCEffect[F])(implicit F: Sync[F]): Flushable[F] =
    new Flushable[F] {
      def flush: Pipe[F, WriteOp, Unit] =
        _.flatMap {
          case JDBCWriteOp(sql, binder) =>
            C.executePreparedQuery(sql, binder)
        }.drain
    }

  def connectionFromConfig(config: Config = ConfigFactory.load()): Connection = {
    val jdbcConfig = config.getConfig("simpledba.jdbc")
    val jdbcUrl    = jdbcConfig.getString("url")
    if (jdbcConfig.hasPath("credentials")) {
      val cc = jdbcConfig.getConfig("credentials")
      DriverManager.getConnection(jdbcUrl, cc.getString("username"), cc.getString("password"))
    } else DriverManager.getConnection(jdbcUrl)
  }

}
