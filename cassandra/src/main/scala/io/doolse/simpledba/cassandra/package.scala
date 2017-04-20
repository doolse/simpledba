package io.doolse.simpledba

import cats.Monad
import cats.data.Kleisli
import com.datastax.driver.core.schemabuilder.Create
import fs2.util.Catchable
import fs2.{Stream, Task}
import io.doolse.simpledba.cassandra.CassandraIO.strat

/**
  * Created by jolz on 10/06/16.
  */
package object cassandra {

  type Effect[A] = Kleisli[Task, CassandraSession, A]
  type CassandraDDL = (String, Create)

  implicit val cassandraFlusher : Flushable[Effect] = new Flushable[Effect] {
    import fs2.interop.cats._
    def flush[A](f: Stream[Effect, WriteOp]): Effect[Unit] = Kleisli {
      s => f.through(CassandraIO.writePipe).run.run(s)
    }
  }

  object stdImplicits {
    implicit val taskMonad : Monad[Task] = fs2.interop.cats.monadToCats[Task]
    implicit val cassandraEffectMonad : Monad[Effect] = Kleisli.catsDataMonadReaderForKleisli
    implicit val asyncInstance : Catchable[Effect] = fs2.interop.cats.kleisliCatchableInstance
  }
}
