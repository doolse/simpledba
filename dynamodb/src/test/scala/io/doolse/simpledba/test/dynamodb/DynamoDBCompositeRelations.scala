package io.doolse.simpledba.test.dynamodb

import java.util.UUID

import io.doolse.simpledba.{AutoConvert, Cols}
import io.doolse.simpledba.dynamodb.{DynamoDBEffect, DynamoDBWriteOp}
import io.doolse.simpledba.test.CompositeRelations
import io.doolse.simpledba.test.CompositeRelations.{Composite2, Composite3}
import io.doolse.simpledba.test.zio.ZIOProperties
import zio.stream._
import zio.{Task, ZIO}
import zio.interop.catz._
import io.doolse.simpledba.interop.zio._

object DynamoDBCompositeRelations extends CompositeRelations[Stream[Throwable, ?], Task, DynamoDBWriteOp]("DynamoDB Composite")
  with ZIOProperties with DynamoDBTestHelper[Stream[Throwable, ?], Task] {

  override def effect = DynamoDBEffect[S, Task](ZIO.succeed(localClient))

  lazy val queries2: Queries2 = {
    import mapper.queries._
    val table = mapper.mapped[Composite2].table("composite2").partKey('pkLong).sortKey('pkUUID)
    run(delAndCreate(table))
    Queries2(writes(table), get(table).build[(Long, UUID)], Stream.empty)
  }
  lazy val queries3: Queries3 = {
    import mapper.queries._
    val table = mapper.mapped[Composite3].table("composite3").partKeys(Cols('pkInt, 'pkString, 'pkBool))
    run(delAndCreate(table))
    Queries3(writes(table), get(table).build[(Int, String, Boolean)], Stream.empty)
  }

}
