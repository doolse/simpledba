package io.doolse.simpledba

import cats.Monad
import cats.data.Kleisli
import fs2.{Async, Task}
import io.doolse.simpledba.dynamodb.DynamoDBIO.strat
import io.doolse.simpledba.dynamodb.DynamoDBMapper.Effect

/**
  * Created by jolz on 14/06/16.
  */
package object dynamodb {

  object stdImplicits {
    implicit val taskMonad : Monad[Task] = fs2.interop.cats.monadToCats[Task]
    implicit val dynamoDBEffectMonad : Monad[Effect] = Kleisli.kleisliMonadReader[Task, DynamoDBSession]
    implicit val asyncInstance : Async[Effect] = CatsUtils.readerCatchable[DynamoDBSession]
  }
}
