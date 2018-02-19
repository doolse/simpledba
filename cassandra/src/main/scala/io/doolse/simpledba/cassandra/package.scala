package io.doolse.simpledba

import cats.Monad
import cats.data.{Kleisli, StateT}
import cats.effect.IO
import com.datastax.driver.core.schemabuilder.Create
import fs2.Stream

/**
  * Created by jolz on 10/06/16.
  */
package object cassandra {

  type Effect[A] = StateT[IO, CassandraSession, A]
  type CassandraDDL = (String, Create)

  implicit val cassandraFlusher : Flushable[Effect] = new Flushable[Effect] {
    def flush[A](f: Stream[Effect, WriteOp]): Effect[Unit] = StateT.inspectF {
      s => f.through(CassandraIO.writePipe).compile.drain.runA(s)
    }
  }
}
