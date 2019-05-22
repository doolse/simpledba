package io.doolse.simpledba.test

import java.sql.Connection

import cats.effect.IO
import fs2.Stream
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.{Cols, Flushable, Streamable}
import shapeless.{::, HList, HNil}
import shapeless.syntax.singleton._
import io.doolse.simpledba.fs2._

trait JDBCTester[C[_] <: JDBCColumn] extends StdColumns[C] with Test[fs2.Stream, IO] {

  type F[A] = IO[A]

  def connection: Connection
  def effect: JDBCEffect[fs2.Stream, F] = ConnectedEffect(connection, ConsoleLogger())
  def last[A](s: fs2.Stream[F, A]) = s.last
  def S = effect.S
  def M = effect.M
  def mapper: JDBCMapper[C]

  implicit def cols = mapper.mapped[EmbeddedFields].embedded

  val instTable = mapper.mapped[Inst].table("inst").key('uniqueid)
  val userTable = mapper.mapped[User].table("user").keys(Cols('firstName, 'lastName))

  implicit def flusher : Flushable[fs2.Stream, F] = effect.flushable

  def insertInst: (Long => Inst) => Stream[F, Inst]

  def makeQueries = {
    val builder = mapper.queries(effect)

    import builder._

    val schemaSQL = mapper.dialect
    Queries(
      effect.flushable.flush {
        Stream(instTable, userTable).flatMap { t =>
          val d = t.definition
          rawSQLStream(Stream(schemaSQL.dropTable(d), schemaSQL.createTable(d)))
        }
      }.compile.drain,
      writes(instTable),
      writes(userTable),
      insertInst,
      byPK(instTable),
      query(userTable)
        .where('firstName, BinOp.EQ)
        .orderWith(HList('lastName ->> false, 'year ->> false))
        .build[String],
      query(userTable).where('lastName, BinOp.EQ).orderBy('year, true).build[String],
      byPK(userTable),
      selectFrom(userTable)
        .cols(Cols('year))
        .where(userTable.keyNames, BinOp.EQ)
        .buildAs[Username, Int],
      selectFrom(userTable).count.where('lastName, BinOp.EQ).buildAs[String, Int]
    )
  }
}
