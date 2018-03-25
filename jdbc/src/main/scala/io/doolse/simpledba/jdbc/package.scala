package io.doolse.simpledba

import java.sql.{Connection, PreparedStatement}

import cats.data.{Kleisli, StateT}
import cats.effect.IO

package object jdbc {

  type JDBCIO[A] = StateT[IO, Connection, A]

  type BindFunc = Kleisli[StateT[IO, Int, ?],
      (() => String, JDBCSQLDialect, Connection, PreparedStatement), Unit]

  implicit val flusher : Flushable[JDBCIO] = new Flushable[JDBCIO] {
    def flush = JDBCQueries.flush
  }
}
