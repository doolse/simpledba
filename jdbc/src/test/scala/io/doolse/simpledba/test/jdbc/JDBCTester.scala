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

trait JDBCTester[C[A] <: JDBCColumn[A]] extends StdColumns[C] with Test[fs2.Stream[IO, ?], IO, JDBCWriteOp]
  with FS2Properties {

  type F[A] = IO[A]

  def connection: Connection
  def effect: JDBCEffect[fs2.Stream[IO, ?], F] = JDBCEffect(IO.pure(connection), _ => IO.pure(), ConsoleLogger())
  def mapper: JDBCMapper[C]
  def builder = mapper.queries(effect)

  override def flush(s: Stream[IO, JDBCWriteOp]): IO[Unit] = builder.flush(s)

  implicit def cols = mapper.mapped[EmbeddedFields].embedded

  val instTable = mapper.mapped[Inst].table("inst").key('uniqueid)
  val userTable = mapper.mapped[User].table("user").keys(Cols('firstName, 'lastName))


  def insertInst: (Long => Inst) => F[Inst]

  def makeQueries = {

    val b = builder
    import b.{flush => _, _}

    val schemaSQL = mapper.dialect
    Queries(
      flush {
        Stream(instTable, userTable).flatMap { t =>
          val d = t.definition
          rawSQLStream(Stream(schemaSQL.dropTable(d), schemaSQL.createTable(d)))
        }
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
