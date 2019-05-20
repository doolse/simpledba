package io.doolse.simpledba.test

import java.sql.DriverManager

import fs2.Stream
import io.doolse.simpledba.Cols
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.jdbc.oracle._
import io.doolse.simpledba.syntax._
import io.doolse.simpledba.test.Test._
import shapeless._
import shapeless.syntax.singleton._

object OracleTester extends App {

  val connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:OraDoc",
                                               "simpledba",
                                               "simpledba123")

  val mapper           = oracleMapper
  val effect           = StateIOEffect(ConsoleLogger())
  val schemaSQL        = mapper.dialect
  implicit val flusher = effect.flushable
  import mapper.mapped

  implicit val cols = mapped[EmbeddedFields].embedded

  val instTable = mapped[Inst].table("inst").key('uniqueid)
  val userTable = mapped[User].table("user").keys(Cols('firstName, 'lastName))

  val seq = Sequence[Long]("ids")

  val builder       = mapper.queries(effect)
  val oracleQueries = new OracleQueries(schemaSQL, effect)
  import builder._
  val q = Queries[JDBCIO](
    writes(instTable),
    writes(userTable),
    oracleQueries.insertWith(instTable, seq),
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

  val prog = for {
    t <- Stream(instTable, userTable).flatMap { t =>
      val d = t.definition
      rawSQLStream(Stream(schemaSQL.dropTable(d), schemaSQL.createTable(d)))
    }.flush
    r <- Test.doTest(q)
  } yield r

  println(prog.compile.last.runA(connection).unsafeRunSync())

}
