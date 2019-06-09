package io.doolse.simpledba.dynamodb.test

import cats.MonadError
import io.doolse.simpledba.dynamodb.DynamoDBEffect
import io.doolse.simpledba.ziointerop._
import zio.console._
import zio.interop.catz._
import zio.{App, Task, ZIO}
import zio.stream.{Stream, ZSink}

object DynamoDBZIOTest extends App with DynamoDBTest[Stream[Throwable, ?], Task] {
  override def effect = DynamoDBEffect[Stream[Throwable, ?], Task](ZIO.succeed(localClient))

  override def AE: MonadError[Task, Throwable] = implicitly[MonadError[Task, Throwable]]

  override def run(args: List[String]): ZIO[DynamoDBZIOTest.Environment, Nothing, Int] = {
    prog.runCollect
      .flatMap(a => putStrLn(a.toString))
      .fold(t => {
        t.printStackTrace()
        1
      }, _ => 0)
  }
}
