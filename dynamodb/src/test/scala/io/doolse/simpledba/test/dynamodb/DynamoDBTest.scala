package io.doolse.simpledba.test.dynamodb

import cats.syntax.flatMap._
import cats.syntax.functor._
import io.doolse.simpledba.test.Test
import io.doolse.simpledba.{Cols, Flushable}

case class MyTest(name: String, frogs: Int)

trait DynamoDBTest[S[_], F[_]] extends Test[S, F] with DynamoDBTestHelper[S, F] {

  implicit val embedded = mapper.mapped[EmbeddedFields].embedded
  val userLNTable       = mapper.mapped[User].table("userLN", 'lastName, 'firstName)
  val userTable =
    mapper
      .mapped[User]
      .table("user", 'firstName, 'lastName)
      .withLocalIndex('yearIndex, 'year)
  val instTable = mapper.mapped[Inst].table("inst", 'uniqueid)

  override def flusher: Flushable[S] = mapper.flusher

  val q = mapper.queries
  import q._

  val tables = S.emits(Seq(userTable, userLNTable, instTable))
  val writeInst = writes(instTable)
  val queries = {
    implicit def M = effect.S.M
    Queries(
      S.drain(S.evalMap(tables)(delAndCreate)),
      writeInst,
      writes(userTable, userLNTable),
      f => {
        val inst = f(1L)
        flusher.flush(writeInst.insert(inst)).map(_ => inst)
      },
      get(instTable).build,
      queryIndex(userTable, 'yearIndex).build(true),
      query(userLNTable).build(false),
      get(userTable).build,
      getAttr(userTable, Cols('year)).buildAs[Username, Int],
      query(userLNTable).count
    )
  }

  val prog = for {
    _   <- S.eval(queries.initDB)
    res <- doTest(queries)
  } yield res

}
