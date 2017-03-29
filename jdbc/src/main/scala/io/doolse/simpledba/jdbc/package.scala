package io.doolse.simpledba

import cats.{Monad, Monoid}
import cats.data.{Kleisli, ReaderT, WriterT}
import fs2.Task
import fs2.util.Catchable
import fs2._
import CatsUtils._

/**
  * Created by jolz on 12/03/17.
  */
package object jdbc {

  type JDBCWriter[A] = WriterT[Task, Stream[Pure, JDBCWrite], A]

  implicit val jdbcWriterMonoid : Monoid[Stream[Pure, JDBCWrite]] = CatsUtils.streamMonoid[Pure, JDBCWrite]

  type Effect[A] = Kleisli[JDBCWriter, JDBCSession, A]
  type JDBCDDL = JDBCCreateTable

  implicit val jdbcFlushable : Flushable[Effect] = new Flushable[Effect] {
    import fs2.interop.cats._
    override def flush[A](f: Effect[A]): Effect[A] = Kleisli(s => WriterT.lift(JDBCIO.runWrites(f).run(s)))
  }

  object stdImplicits {
    implicit val taskMonad : Monad[Task] = fs2.interop.cats.monadToCats[Task]
    implicit val jdbcEffectMonad : Monad[Effect] = Kleisli.catsDataMonadReaderForKleisli
    implicit val asyncInstance : Catchable[Effect] = fs2.interop.cats.kleisliCatchableInstance[JDBCWriter, JDBCSession]
  }

}
