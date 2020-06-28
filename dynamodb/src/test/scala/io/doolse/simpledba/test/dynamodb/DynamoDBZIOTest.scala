package io.doolse.simpledba.test.dynamodb

import io.doolse.simpledba.dynamodb.DynamoDBEffect
import io.doolse.simpledba.interop.zio._
import io.doolse.simpledba.test.zio.ZIOProperties
import zio.console._
import zio.{App, ExitCode, Task, ZIO}

object DynamoDBZIOTest
    extends App
    with DynamoDBTest[StreamTask, Task]
    with ZIOProperties {
  override def effect = DynamoDBEffect[StreamTask, Task](ZIO.succeed(localClient))

  override def run(args: List[String]) = {
    prog.tap(a => putStrLn(a)) fold (t => {
      t.printStackTrace();
      ExitCode.failure
    }, _ => ExitCode.success)
  }
}
