package io.doolse.simpledba

import cats.data.Kleisli
import cats.{Functor, Monad}
import fs2.Task
import fs2.util.{Async, Catchable}
import io.doolse.simpledba.cassandra.CassandraMapper.Effect
import io.doolse.simpledba.cassandra.CassandraIO.strat

/**
  * Created by jolz on 10/06/16.
  */
package object cassandra {

  object stdImplicits {
    implicit val taskMonad : Monad[Task] = fs2.interop.cats.monadToCats[Task]
    implicit val cassandraEffectMonad : Monad[Effect] = Kleisli.catsDataMonadReaderForKleisli
    implicit val asyncInstance : Catchable[Effect] = fs2.interop.cats.kleisliCatchableInstance
  }
}
