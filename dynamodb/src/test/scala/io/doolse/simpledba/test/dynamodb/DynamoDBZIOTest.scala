package io.doolse.simpledba.test.dynamodb

import io.doolse.simpledba.dynamodb.DynamoDBEffect
import io.doolse.simpledba.test.zio.ZIOProperties
import zio.console._
import zio.{App, Task, ZIO}
import zio.stream.{Stream, ZSink}
import io.doolse.simpledba.interop.zio._

object DynamoDBZIOTest
    extends App
    with DynamoDBTest[Stream[Throwable, ?], Task]
    with ZIOProperties {
  override def effect = DynamoDBEffect[Stream[Throwable, ?], Task](ZIO.succeed(localClient))

  override def run(args: List[String]): ZIO[DynamoDBZIOTest.Environment, Nothing, Int] = {
    prog.fold(t => {
      t.printStackTrace()
      1
    }, _ => 0)
  }
}
