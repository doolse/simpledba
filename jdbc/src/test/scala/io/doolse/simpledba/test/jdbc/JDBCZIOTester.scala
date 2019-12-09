package io.doolse.simpledba.test.jdbc

import java.sql.Connection

import io.doolse.simpledba.Cols
import io.doolse.simpledba.interop.zio._
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.test.Test
import io.doolse.simpledba.test.zio.ZIOProperties
import shapeless.HList
import shapeless.syntax.singleton._
import zio.interop.catz._
import zio.stream.Stream
import zio.{RIO, Task}

trait JDBCZIOTester extends StdColumns with Test[Stream[Throwable, *], Task, JDBCWriteOp]
  with ZIOProperties {

  def connection: Connection
  def effect = JDBCEffect.withLogger[Stream[Throwable, *], Task](providedJDBCConnection(connection), PrintLnLogger())
  def mapper: JDBCMapper[C]
  def builder = mapper.queries(effect)

  override def flush(s: Stream[Throwable, JDBCWriteOp]): Task[Unit] = builder.flush(s)

  implicit def cols = mapper.mapped[EmbeddedFields].embedded

  val instTable = mapper.mapped[Inst].table("inst").key('uniqueid)
  val userTable = mapper.mapped[User].table("user").keys(Cols('firstName, 'lastName))


  def insertInst: (Long => Inst) => Task[Inst]

  def makeQueries = {

    val b = builder
    val S = streamable
    import b.{flush => _, _}

    Queries(
      flush {
        S.emits(Seq(instTable, userTable)).flatMap(ddl.dropAndCreate)
      },
      writes(instTable),
      writes(userTable),
      insertInst,
      byPK(instTable).build,
      query(userTable)
        .where('firstName, BinOp.EQ)
        .orderWith(HList('lastName ->> false, 'year ->> false))
        .build[String],
      query(userTable).where('lastName, BinOp.EQ).orderBy('year, true).build[String],
      byPK(userTable).build,
      selectFrom(userTable)
        .cols(Cols('year))
        .where(userTable.keyNames, BinOp.EQ)
        .buildAs[Username, Int],
      selectFrom(userTable).count.where('lastName, BinOp.EQ).buildAs[String, Int]
    )
  }
}
