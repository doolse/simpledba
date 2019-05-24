package io.doolse.simpledba.dynamodb.test

import cats.MonadError
import io.doolse.simpledba.dynamodb.DynamoDBEffect
import io.doolse.simpledba.zio._
import scalaz.zio.console._
import scalaz.zio.interop.catz._
import scalaz.zio.{App, Task, ZIO}
import scalaz.zio.stream.{Stream, ZSink}

object DynamoDBZIOTest extends App with DynamoDBTest[Stream[Throwable, ?], Task] {
  override def effect = DynamoDBEffect[Stream[Throwable, ?], Task](ZIO.succeed(localClient))

  override def AE: MonadError[Task, Throwable] = implicitly[MonadError[Task, Throwable]]

  override def run(args: List[String]): ZIO[DynamoDBZIOTest.Environment, Nothing, Int] = {
    prog.run(ZSink.collect[String])
      .flatMap(a => putStrLn(a.toString))
      .fold(t => {
        t.printStackTrace()
        1
      }, _ => 0)
  }
}
