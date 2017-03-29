package io.doolse.simpledba

import cats.Monad
import cats.data.{Kleisli, ReaderT, WriterT}
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import fs2.interop.cats.kleisliCatchableInstance
import fs2.{Stream, Pure, Task}
import fs2.util.{Async, Catchable}
import io.doolse.simpledba.dynamodb.DynamoDBIO.strat
import CatsUtils._

/**
  * Created by jolz on 14/06/16.
  */
package object dynamodb {

  type DynamoDBWriter[A] = WriterT[Task, Stream[Pure, DynamoDBWrite], A]
  type Effect[A] = ReaderT[DynamoDBWriter, DynamoDBSession, A]
  type DynamoDBDDL = CreateTableRequest

  implicit val dyamoWritesMonoid = CatsUtils.streamMonoid[Pure, DynamoDBWrite]

  object stdImplicits {
    implicit val taskMonad : Monad[Task] = fs2.interop.cats.monadToCats[Task]
    implicit val dynamoDBEffectMonad : Monad[Effect] = Kleisli.catsDataMonadReaderForKleisli[DynamoDBWriter, DynamoDBSession]
    implicit val catchableInstance : Catchable[Effect] = kleisliCatchableInstance[DynamoDBWriter, DynamoDBSession]
  }

  implicit val dynamoDBFlushable : Flushable[Effect] = new Flushable[Effect] {
    import fs2.interop.cats._
    override def flush[A](f: Effect[A]): Effect[A] = Kleisli(s => WriterT.lift(DynamoDBIO.runWrites(f).run(s)))
  }

}
