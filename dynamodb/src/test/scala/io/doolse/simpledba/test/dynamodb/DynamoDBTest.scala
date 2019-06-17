package io.doolse.simpledba.test.dynamodb

import cats.syntax.flatMap._
import cats.syntax.functor._
import io.doolse.simpledba.test.Test
import io.doolse.simpledba.{Cols, Flushable}

case class MyTest(name: String, frogs: Int)

trait DynamoDBTest[S[_], F[_]] extends Test[S, F] with DynamoDBTestHelper[S, F] {

  implicit val embedded = mapper.mapped[EmbeddedFields].embedded
  val userLNTable       = mapper.mapped[User].table("userLN").partKey('lastName).sortKey('firstName)
  val userTable =
    mapper
      .mapped[User]
      .table("user").partKey('firstName).sortKey('lastName)
      .withLocalIndex('yearIndex, 'year)
  val instTable = mapper.mapped[Inst].table("inst").partKey('uniqueid)

  override def flusher: Flushable[S] = mapper.flusher

  val q = mapper.queries
  import q._

  val tables = streamable.emits(Seq(userTable, userLNTable, instTable))
  val writeInst = writes(instTable)
  val queries = {
    implicit def M = effect.S.M
    Queries(
      streamable.drain(streamable.evalMap(tables)(delAndCreate)),
      writeInst,
      writes(userTable, userLNTable),
      f => {
        val inst = f(1L)
        flusher.flush(writeInst.insert(inst)).map(_ => inst)
      },
      get(instTable).build[Long],
      ???,
//      queryIndex(userTable, 'yearIndex).build(true)[Int],
      query(userLNTable).build(false),
      get(userTable).build,
      getAttr(userTable, Cols('year)).buildAs[Username, Int],
      query(userLNTable).count
    )
  }

  val prog = for {
    _   <- streamable.eval(queries.initDB)
    res <- doTest(queries)
  } yield res

}
