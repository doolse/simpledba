package io.doolse.simpledba2

import java.sql.Connection

import cats.data.StateT
import cats.effect.IO

package object jdbc {

  type JDBCIO[A] = StateT[IO, Connection, A]


  implicit val flusher : Flushable[JDBCIO] = new Flushable[JDBCIO] {
    def flush = JDBCQueries.flush
  }
}
