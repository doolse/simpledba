package io.doolse.simpledba.test

import java.sql.DriverManager

import cats.effect.IO
import fs2.Stream
import io.doolse.simpledba.Cols
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.jdbc.postgres._
import Test._
import io.doolse.simpledba.syntax._
import shapeless._
import shapeless.syntax.singleton._

object PostgresTester extends App {

  val connection = DriverManager.getConnection("jdbc:postgresql:simpledba2", "equellauser", "tle010")

  val mapper = postgresMapper
  val effect = StateIOEffect(ConsoleLogger())
  val schemaSQL       = mapper.dialect
  implicit val flusher = effect.flushable
  import mapper.mapped

  val seq = Sequence[Long]("uniquevals")
  implicit val cols = mapped[EmbeddedFields].embedded
  val instTable = mapped[Inst].table("inst").key('uniqueid)
  val userTable = mapped[User].table("user").keys(Cols('firstName, 'lastName))

  val builder = mapper.queries(effect)
  val postgresQueries = new PostgresQueries(schemaSQL, effect)
  import builder._
  val q = Queries[JDBCIO](writes(instTable), writes(userTable), postgresQueries.insertWith(instTable, seq),
    byPK(instTable),
    query(userTable).where('firstName, BinOp.EQ).orderWith(HList('lastName ->> false,
      'year ->> false)).build[String],
    query(userTable).where('lastName, BinOp.EQ).orderBy('year, true).build[String],
    byPK(userTable),
    selectFrom(userTable).cols(Cols('year)).where(userTable.keyNames, BinOp.EQ).buildAs[Username, Int],
    selectFrom(userTable).count.where('lastName, BinOp.EQ).buildAs[String, Int]
  )

  val prog = for {
    t <- Stream(instTable, userTable).flatMap { t =>
      val d = t.definition
      rawSQLStream(Stream(schemaSQL.dropTable(d), schemaSQL.createTable(d)))
    }.flush
    r <- Test.doTest(q)
  } yield r

  println(prog.compile.last.runA(connection).unsafeRunSync())


}
