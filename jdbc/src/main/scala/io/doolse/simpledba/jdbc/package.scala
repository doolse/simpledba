package io.doolse.simpledba

import java.sql.{Connection, DriverManager, PreparedStatement}

import cats.{Applicative, Monad}
import cats.data.{Kleisli, State, StateT}
import cats.effect.{IO, Sync}
import cats.syntax.applicative._
import com.typesafe.config.{Config, ConfigFactory}

package object jdbc {

  type BindFunc[A] = Kleisli[State[Int, ?], (Connection, PreparedStatement), A]

  type ParamBinder = (Int, Connection, PreparedStatement) => Unit

  type JDBCIO[A] = StateT[IO, Connection, A]

  case class ConnectedEffect[S[_[_], _], F[_]](connection: Connection,
      logger: JDBCLogger[F])(implicit val S: Streamable[S, F], Sync : Sync[F])
      extends JDBCEffect[S, F] {
    def M = Sync
    override def acquire: F[Connection] = Sync.pure(connection)

    override def release: Connection => F[Unit] = _ => Sync.pure()

    override def blockingIO[A](thunk: => A): F[A] = Sync.delay(thunk)

    override def flushable: Flushable[S, F] = flushJDBC(this)
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
