package io.doolse.simpledba

import cats.Monad
import cats.data.Kleisli
import fs2.{Task, _}
import fs2.util.Catchable

/**
  * Created by jolz on 12/03/17.
  */
package object jdbc {

  type Effect[A] = Kleisli[Task, JDBCSession, A]
  type JDBCDDL = JDBCCreateTable

  implicit val jdbcFlusher : Flushable[Effect] = new Flushable[Effect] {
    import fs2.interop.cats._
    def flush[A](f: Stream[Effect, WriteOp]): Effect[Unit] = Kleisli {
      s => f.to(JDBCIO.writeSink).run.run(s)
    }
  }

  object stdImplicits {
    implicit val taskMonad : Monad[Task] = fs2.interop.cats.monadToCats[Task]
    implicit val jdbcEffectMonad : Monad[Effect] = Kleisli.catsDataMonadReaderForKleisli
    implicit val asyncInstance : Catchable[Effect] = fs2.interop.cats.kleisliCatchableInstance[Task, JDBCSession]
  }

}
