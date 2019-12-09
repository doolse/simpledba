package io.doolse.simpledba.test.jdbc

import java.sql.Connection

import cats.effect.IO
import fs2.Stream
import io.doolse.simpledba.Cols
import io.doolse.simpledba.fs2._
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.test.Test
import shapeless.HList
import shapeless.syntax.singleton._

trait JDBCTester extends StdColumns with Test[fs2.Stream[IO, *], IO, JDBCWriteOp]
  with FS2Properties {

  def connection: Connection
  def effect  =
    JDBCEffect[fs2.Stream[IO, *], IO](providedJDBCConnection(connection))
  def mapper: JDBCMapper[C]
  def builder = mapper.queries(effect)

  override def flush(s: Stream[IO, JDBCWriteOp]): IO[Unit] = builder.flush(s)

  implicit def cols = mapper.mapped[EmbeddedFields].embedded

  val instTable = mapper.mapped[Inst].table("inst").key('uniqueid)
  val userTable = mapper.mapped[User].table("user").keys(Cols('firstName, 'lastName))


  def insertInst: (Long => Inst) => IO[Inst]

  def makeQueries = {

    val b = builder
    import b.{flush => _, _}

    Queries(
      flush {
        Stream(instTable, userTable).flatMap(ddl.dropAndCreate)
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
