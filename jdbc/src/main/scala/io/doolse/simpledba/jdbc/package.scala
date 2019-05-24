package io.doolse.simpledba

import java.sql.{Connection, DriverManager, PreparedStatement}

import cats.data.{Kleisli, State, StateT}
import cats.effect.IO
import com.typesafe.config.{Config, ConfigFactory}

package object jdbc {

  type BindFunc[A] = Kleisli[State[Int, ?], (Connection, PreparedStatement), A]

  type ParamBinder = (Int, Connection, PreparedStatement) => Unit

  type JDBCIO[A] = StateT[IO, Connection, A]

  def connectionFromConfig(config: Config = ConfigFactory.load()): Connection = {
    val jdbcConfig = config.getConfig("simpledba.jdbc")
    val jdbcUrl    = jdbcConfig.getString("url")
    if (jdbcConfig.hasPath("credentials")) {
      val cc = jdbcConfig.getConfig("credentials")
      DriverManager.getConnection(jdbcUrl, cc.getString("username"), cc.getString("password"))
    } else DriverManager.getConnection(jdbcUrl)
  }

}
