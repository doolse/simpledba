package io.doolse.simpledba

import cats.data.Kleisli
import cats.{Functor, Monad}
import fs2.{Async, Task}
import io.doolse.simpledba.cassandra.CassandraMapper.Effect
import io.doolse.simpledba.cassandra.CassandraIO.strat

/**
  * Created by jolz on 10/06/16.
  */
package object cassandra {

  object stdImplicits {
    implicit val taskMonad : Monad[Task] = fs2.interop.cats.monadToCats[Task]
    implicit val dynamoDBEffectMonad : Monad[Effect] = Kleisli.kleisliMonadReader[Task, CassandraSession]
    implicit val asyncInstance : Async[Effect] = CatsUtils.readerCatchable[CassandraSession]
  }
}
