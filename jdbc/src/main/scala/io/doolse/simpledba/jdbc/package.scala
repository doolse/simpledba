package io.doolse.simpledba

import cats.Monad
import cats.data.{Kleisli, StateT}
import cats.effect.IO
import fs2.Stream

/**
  * Created by jolz on 12/03/17.
  */
package object jdbc {

  type Effect[A] = StateT[IO, JDBCSession, A]
  type JDBCDDL = JDBCCreateTable

  implicit val jdbcFlusher : Flushable[Effect] = new Flushable[Effect] {
    def flush[A](f: Stream[Effect, WriteOp]): Effect[Unit] = StateT.inspectF {
      s => f.to(JDBCIO.writeSink).compile.drain.runA(s)
    }
  }
}
