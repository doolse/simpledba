package io.doolse.simpledba

import cats.Monad
import cats.data.{Kleisli, ReaderT, StateT}
import cats.effect.IO
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import fs2.Stream

/**
  * Created by jolz on 14/06/16.
  */
package object dynamodb {

  type Effect[A] = StateT[IO, DynamoDBSession, A]
  type DynamoDBDDL = CreateTableRequest

  implicit val dynamoFlusher : Flushable[Effect] = new Flushable[Effect] {
    def flush[A](f: Stream[Effect, WriteOp]): Effect[Unit] = StateT.inspectF {
      s => f.through(DynamoDBIO.writePipe).compile.drain.runA(s)
    }
  }
}
