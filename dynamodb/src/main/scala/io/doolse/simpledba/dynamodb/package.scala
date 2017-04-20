package io.doolse.simpledba

import cats.Monad
import cats.data.{Kleisli, ReaderT}
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import fs2.interop.cats.kleisliCatchableInstance
import fs2.util.Catchable
import fs2.{Stream, Task}
import io.doolse.simpledba.dynamodb.DynamoDBIO.strat

/**
  * Created by jolz on 14/06/16.
  */
package object dynamodb {

  type Effect[A] = ReaderT[Task, DynamoDBSession, A]
  type DynamoDBDDL = CreateTableRequest

  implicit val dynamoFlusher : Flushable[Effect] = new Flushable[Effect] {
    import fs2.interop.cats._
    def flush[A](f: Stream[Effect, WriteOp]): Effect[Unit] = Kleisli {
      s => f.through(DynamoDBIO.writePipe).run.run(s)
    }
  }

  object stdImplicits {
    implicit val taskMonad : Monad[Task] = fs2.interop.cats.monadToCats[Task]
    implicit val dynamoDBEffectMonad : Monad[Effect] = Kleisli.catsDataMonadReaderForKleisli[Task, DynamoDBSession]
    implicit val catchableInstance : Catchable[Effect] = kleisliCatchableInstance[Task, DynamoDBSession]
  }

}
