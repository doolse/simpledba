package io.doolse.simpledba.jdbc.test

import java.sql.DriverManager

import fs2.Stream
import io.doolse.simpledba.{Cols, Flushable}
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.jdbc.oracle._
import io.doolse.simpledba.jdbc.test.Test._
import io.doolse.simpledba.syntax._
import shapeless._
import shapeless.syntax.singleton._

object OracleTester extends App {

  val connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:OraDoc",
                                               "simpledba",
                                               "simpledba123")

  implicit val config = oracleConfig.withBindingLogger((sql,vals) => println(s"$sql $vals"))
  val schemaSQL       = config.schemaSQL
  implicit val cols   = TableMapper[EmbeddedFields].embedded

  val instTable = TableMapper[Inst].table("inst").key('uniqueid)
  val userTable = TableMapper[User].table("user").keys(Cols('firstName, 'lastName))

  val seq = Sequence[Long]("ids")

  val builder = new JDBCQueries[JDBCIO2]
  val oracleQueries = new OracleQueries[JDBCIO2]
  import builder._
  val q = Queries[JDBCIO2](
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
      rawSQLStream[JDBCIO2](Stream(schemaSQL.dropTable(d), schemaSQL.createTable(d)))
    }.flush
    r <- Test.doTest(q)
  } yield r

  println(prog.compile.last.runA(connection).unsafeRunSync())

}
