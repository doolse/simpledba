package io.doolse.simpledba

import cats.Monad
import cats.data.Kleisli
import fs2.interop.cats.kleisliCatchableInstance
import fs2.Task
import fs2.util.{Async, Catchable}
import io.doolse.simpledba.dynamodb.DynamoDBIO.strat
import io.doolse.simpledba.dynamodb.DynamoDBMapper.Effect

/**
  * Created by jolz on 14/06/16.
  */
package object dynamodb {

  object stdImplicits {
    implicit val taskMonad : Monad[Task] = fs2.interop.cats.monadToCats[Task]
    implicit val dynamoDBEffectMonad : Monad[Effect] = Kleisli.catsDataMonadReaderForKleisli[Task, DynamoDBSession]
    implicit val catchableInstance = kleisliCatchableInstance[Task, DynamoDBSession]
  }
}
