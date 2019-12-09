package io.doolse.simpledba.test.dynamodb

import cats.syntax.all._
import io.doolse.simpledba.Cols
import io.doolse.simpledba.dynamodb.DynamoDBWriteOp
import io.doolse.simpledba.test.Test

case class MyTest(name: String, frogs: Int)

trait DynamoDBTest[S[_], F[_]] extends Test[S, F, DynamoDBWriteOp] with DynamoDBTestHelper[S, F] {

  val userLNTable       = mapper.mapped[User].table("userLN")
    .partKey('lastName).sortKey('firstName)
  val userTable =
    mapper
      .mapped[User]
      .table("user").partKey('firstName).sortKey('lastName)
      .withLocalIndex('yearIndex, 'year)

  implicit val embedded = mapper.mapped[EmbeddedFields].embedded
  val instTable = mapper.mapped[Inst].table("inst").partKey('uniqueid)

  val q = mapper.queries
  import q._

  val tables = streamable.emits(Seq(userTable, userLNTable, instTable))
  val writeInst = writes(instTable)
  implicit val _M = sync
  val queries = {
    Queries(
      streamable.drain(streamable.evalMap(tables)(delAndCreate)),
      writeInst,
      writes(userTable, userLNTable),
      f => {
        val inst = f(1L)
        flush(writeInst.insert(inst)).map(_ => inst)
      },
        get(instTable).build[Long],
      queryIndex(userTable, 'yearIndex).build[String](false),
      query(userLNTable).build[String](true),
      get(userTable).build[Username],
      getAttr(userTable, Cols('year)).buildAs[Username, Int],
      query(userLNTable).count[String]
    )
  }

  val prog = for {
    _   <- queries.initDB
    res <- doTest(queries)
  } yield res

}
