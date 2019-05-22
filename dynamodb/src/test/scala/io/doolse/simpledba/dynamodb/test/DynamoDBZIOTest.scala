package io.doolse.simpledba.dynamodb.test

import java.util.concurrent.CompletableFuture

import cats.{Monad, MonadError}
import io.doolse.simpledba.dynamodb.DynamoDBEffect
import io.doolse.simpledba.{Flushable, Streamable}
import scalaz.zio.{App, Task, ZIO}
import scalaz.zio.console._
import scalaz.zio.interop.catz._
import io.doolse.simpledba.fs2._
import scalaz.zio.interop.javaconcurrent._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

object DynamoDBZIOTest extends App with DynamoDBTest[fs2.Stream, Task] {
  override def effect: DynamoDBEffect[fs2.Stream, Task] = new DynamoDBEffect[fs2.Stream, Task] {
    override def S: Streamable[fs2.Stream, Task] = implicitly[Streamable[fs2.Stream, Task]]

    override def M: Monad[Task] = implicitly[Monad[Task]]

    override def asyncClient: Task[DynamoDbAsyncClient] = ZIO.succeed(localClient)

    override def fromFuture[A](future: => CompletableFuture[A]): Task[A] = Task.fromCompletionStage(() => future)
  }

  override def AE: MonadError[Task, Throwable] = implicitly[MonadError[Task, Throwable]]

  override def S: Streamable[fs2.Stream, Task] = effect.S

  override def run(args: List[String]): ZIO[DynamoDBZIOTest.Environment, Nothing, Int] = {
    prog.compile.last.flatMap(a => putStrLn(a.toString)) fold(_ => 1, _ => 0)
  }
}
