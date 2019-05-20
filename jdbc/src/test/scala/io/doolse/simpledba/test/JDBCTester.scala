package io.doolse.simpledba.test

import fs2.Stream
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.test.Test._
import io.doolse.simpledba.{Cols, Flushable}
import shapeless.{HList, HNil, ::}
import shapeless.syntax.singleton._

trait JDBCTester[C[_] <: JDBCColumn] extends StdColumns[C] {

  type F[A] = JDBCIO[A]

  def effect: JDBCEffect[F] = StateIOEffect(ConsoleLogger())
  def mapper: JDBCMapper[C]

  implicit def cols = mapper.mapped[EmbeddedFields].embedded

  val instTable = mapper.mapped[Inst].table("inst").key('uniqueid)
  val userTable = mapper.mapped[User].table("user").keys(Cols('firstName, 'lastName))

  implicit def flusher : Flushable[F] = effect.flushable

  def insertInst: (Long => Inst) => Stream[F, Inst]

  def makeQueries = {
    val builder = mapper.queries(effect)

    import builder._

    val schemaSQL = mapper.dialect
    Queries[F](
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
