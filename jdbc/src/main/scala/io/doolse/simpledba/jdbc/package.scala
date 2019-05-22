package io.doolse.simpledba

import java.sql.{Connection, DriverManager, PreparedStatement}

import cats.Monad
import cats.data.{Kleisli, State, StateT}
import cats.effect.{IO, Sync}
import cats.syntax.applicative._
import com.typesafe.config.{Config, ConfigFactory}

package object jdbc {

  type BindFunc[A] = Kleisli[State[Int, ?], (Connection, PreparedStatement), A]

  type ParamBinder = (Int, Connection, PreparedStatement) => Unit

  type JDBCIO[A] = StateT[IO, Connection, A]

  case class StateIOEffect[S[_[_], _]](
      logger: JDBCLogger[JDBCIO] = new NothingLogger)(implicit val M: Monad[JDBCIO], val S: Streamable[S, JDBCIO])
      extends JDBCEffect[S, JDBCIO] {
    override def acquire: JDBCIO[Connection] = StateT.get

    override def release: Connection => JDBCIO[Unit] = _ => ().pure[JDBCIO]

    override def blockingIO[A](thunk: => A): JDBCIO[A] = StateT.liftF(IO.delay(thunk))

    override def flushable: Flushable[S, JDBCIO] = flushJDBC(this)
  }

  def flushJDBC[S[_[_], _], F[_]](C: JDBCEffect[S, F]): Flushable[S, F] =
    new Flushable[S, F] {
      val S  = C.S
      val SM = S.M
      def flush =
        writes =>
          S.eval(S.drain(SM.flatMap(writes) {
            case JDBCWriteOp(sql, binder) =>
              C.executePreparedQuery(sql, binder)
          }))
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
