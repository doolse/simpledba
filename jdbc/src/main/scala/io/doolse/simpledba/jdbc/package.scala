package io.doolse.simpledba

import cats.Monad
import cats.data.Kleisli
import fs2.Task
import fs2.util.Catchable
import io.doolse.simpledba.jdbc.JDBCMapper.Effect

/**
  * Created by jolz on 12/03/17.
  */
package object jdbc {
  object stdImplicits {
    implicit val taskMonad : Monad[Task] = fs2.interop.cats.monadToCats[Task]
    implicit val jdbcEffectMonad : Monad[Effect] = Kleisli.catsDataMonadReaderForKleisli
    implicit val asyncInstance : Catchable[Effect] = fs2.interop.cats.kleisliCatchableInstance
  }

}
