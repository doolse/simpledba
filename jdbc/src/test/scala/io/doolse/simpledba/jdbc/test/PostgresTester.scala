package io.doolse.simpledba.jdbc.test

import java.sql.DriverManager

import fs2.Stream
import io.doolse.simpledba.Cols
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.jdbc.postgres._
import io.doolse.simpledba.jdbc.test.Test._
import io.doolse.simpledba.syntax._
import shapeless._
import shapeless.syntax.singleton._

object PostgresTester extends App {

  val connection = DriverManager.getConnection("jdbc:postgresql:simpledba2", "equellauser", "tle010")

  implicit val config = postgresConfig.withBindingLogger((sql,vals) => println(s"$sql $vals"))

  val schemaSQL = config.schemaSQL

  val seq = Sequence[Long]("uniquevals")
  implicit val cols = TableMapper[EmbeddedFields].embedded
  val instTable = TableMapper[Inst].table("inst").key('uniqueid)
  val userTable = TableMapper[User].table("user").keys(Cols('firstName, 'lastName))

  val builder = new JDBCQueries[JDBCIO2]
  val postgresQueries = new PostgresQueries[JDBCIO2]
  import builder._
  val q = Queries[JDBCIO2](writes(instTable), writes(userTable), postgresQueries.insertWith(instTable, seq),
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
      rawSQLStream[JDBCIO2](Stream(schemaSQL.dropTable(d), schemaSQL.createTable(d)))
    }.flush
    r <- Test.doTest(q)
  } yield r

  println(prog.compile.last.runA(connection).unsafeRunSync())


}
