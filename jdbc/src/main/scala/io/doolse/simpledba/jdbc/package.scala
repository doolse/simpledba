package io.doolse.simpledba

import java.sql.{Connection, DriverManager, PreparedStatement}

import cats.data.{Kleisli, StateT}
import cats.effect.IO
import com.typesafe.config.{Config, ConfigFactory}
import fs2.{Sink, Stream}

package object jdbc {

  type JDBCIO[A] = StateT[IO, Connection, A]

  type BindFunc[A] = Kleisli[StateT[IO, Int, ?], (Connection, PreparedStatement), A]

  type ParamBinder = (Int, Connection, PreparedStatement) => Unit

  implicit val flusher : Flushable[JDBCIO] = new Flushable[JDBCIO] {
    def flush: Sink[JDBCIO, WriteOp] = JDBCQueries.flush
  }

  def connectionFromConfig(config: Config = ConfigFactory.load()): Connection = {
    val jdbcConfig = config.getConfig("simpledba.jdbc")
    val jdbcUrl = jdbcConfig.getString("url")
    if (jdbcConfig.hasPath("credentials")) {
      val cc = jdbcConfig.getConfig("credentials")
      DriverManager.getConnection(jdbcUrl, cc.getString("username"), cc.getString("password"))
    } else DriverManager.getConnection(jdbcUrl)
  }

  def rawSQL(sql: String)(implicit config: JDBCConfig): WriteOp = {
    JDBCWriteOp(JDBCRawSQL(sql), config, Kleisli.pure(Seq.empty))
  }

  def rawSQLStream(sql: Stream[JDBCIO, String])(implicit config: JDBCConfig): Stream[JDBCIO, WriteOp] = {
    sql.map(rawSQL)
  }
}
