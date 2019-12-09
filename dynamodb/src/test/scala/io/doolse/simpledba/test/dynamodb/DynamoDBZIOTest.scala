package io.doolse.simpledba.test.dynamodb

import io.doolse.simpledba.dynamodb.DynamoDBEffect
import io.doolse.simpledba.interop.zio._
import io.doolse.simpledba.test.zio.ZIOProperties
import zio.console._
import zio.{App, Task, ZIO}

object DynamoDBZIOTest
    extends App
    with DynamoDBTest[StreamTask, Task]
    with ZIOProperties {
  override def effect = DynamoDBEffect[StreamTask, Task](ZIO.succeed(localClient))

  override def run(args: List[String]): ZIO[Console, Nothing, Int] = {
    prog.tap(putStrLn) fold (t => {
      t.printStackTrace()
      1
    }, _ => 0)
  }
}
