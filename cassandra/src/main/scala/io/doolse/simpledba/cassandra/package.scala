package io.doolse.simpledba

import cats.Monad
import cats.data.{Kleisli, WriterT}
import com.datastax.driver.core.schemabuilder.Create
import fs2.{Pure, Task, Stream}
import fs2.util.Catchable
import CatsUtils._
import io.doolse.simpledba.cassandra.CassandraIO.strat

/**
  * Created by jolz on 10/06/16.
  */
package object cassandra {

  type CassandraWriter[A] = WriterT[Task, Stream[Pure, CassandraWriteOperation], A]
  type Effect[A] = Kleisli[CassandraWriter, CassandraSession, A]
  type CassandraDDL = (String, Create)

  implicit val cassandraWriterMonoid = CatsUtils.streamMonoid[Pure, CassandraWriteOperation]

  object stdImplicits {
    implicit val taskMonad : Monad[Task] = fs2.interop.cats.monadToCats[Task]
    implicit val cassandraEffectMonad : Monad[Effect] = Kleisli.catsDataMonadReaderForKleisli
    implicit val asyncInstance : Catchable[Effect] = fs2.interop.cats.kleisliCatchableInstance
  }

  implicit val cassandraFlushable : Flushable[Effect] = new Flushable[Effect] {
    import fs2.interop.cats._
    override def flush[A](f: Effect[A]): Effect[A] = Kleisli(s => WriterT.lift(CassandraIO.runWrites(f).run(s)))
  }

}
