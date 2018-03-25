package io.doolse.simpledba

import java.sql.{Connection, DriverManager, PreparedStatement}

import cats.data.{Kleisli, RWST, StateT}
import cats.effect.IO
import com.typesafe.config.{Config, ConfigFactory}

package object jdbc {

  type JDBCIO[A] = StateT[IO, Connection, A]

  type BindFunc[A] = Kleisli[StateT[IO, Int, ?], (Connection, PreparedStatement), A]

  implicit val flusher : Flushable[JDBCIO] = new Flushable[JDBCIO] {
    def flush = JDBCQueries.flush
  }

  def connectionFromConfig(config: Config = ConfigFactory.load()): Connection = {
    val jdbcConfig = config.getConfig("simpledba.jdbc")
    val jdbcUrl = jdbcConfig.getString("url")
    if (jdbcConfig.hasPath("credentials")) {
      val cc = jdbcConfig.getConfig("credentials")
      DriverManager.getConnection(jdbcUrl, cc.getString("username"), cc.getString("password"))
    } else DriverManager.getConnection(jdbcUrl)
  }

}
