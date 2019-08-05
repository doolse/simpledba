package io.doolse.simpledba.test.dynamodb

import io.doolse.simpledba.dynamodb.DynamoDBEffect
import io.doolse.simpledba.interop.zio._
import io.doolse.simpledba.test.zio.ZIOProperties
import zio.console._
import zio.{App, RIO, ZIO}

object DynamoDBZIOTest
    extends App
    with DynamoDBTest[ZStreamR, RIO]
    with ZIOProperties {
  override def effect = DynamoDBEffect[ZStreamR, RIO, Any](zioStreamEffects, ZIO.succeed(localClient))

  override def run(args: List[String]): ZIO[DynamoDBZIOTest.Environment, Nothing, Int] = {
    prog.tap(putStrLn) fold (t => {
      t.printStackTrace()
      1
    }, _ => 0)
  }
}
