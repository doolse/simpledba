package io.doolse.simpledba.jdbc.test

import java.sql.DriverManager

import fs2.Stream
import io.doolse.simpledba.Cols
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.jdbc.sqlserver._
import io.doolse.simpledba.jdbc.test.OracleTester.userTable
import io.doolse.simpledba.jdbc.test.Test._
import io.doolse.simpledba.syntax._
import shapeless._
import shapeless.syntax.singleton._

object SQLServerTester extends App {

  val connection = DriverManager.getConnection("jdbc:sqlserver://localhost:1433;database=simpledba;", "testuser", "testPassword12")

  implicit val config = sqlServerConfig.withBindingLogger(msg => println(msg()._1))
  val schemaSQL = config.schemaSQL

  implicit val cols = TableMapper[EmbeddedFields].embedded
  val instTable = TableMapper[Inst].table("inst").edit('uniqueid, identityCol[Long]).key('uniqueid)
  val userTable = TableMapper[User].table("user").keys(Cols('firstName, 'lastName))


  val q = Queries(instTable.writes, userTable.writes, insertIdentity(instTable),
    instTable.byPK,
    userTable.query.where('firstName, BinOp.EQ)
      .orderWith(HList('lastName ->> false, 'year ->> false)).build[String],
    userTable.query.where('lastName, BinOp.EQ)
      .orderBy('year, true).build[String],
    userTable.byPK,
    userTable.select(Cols('year)).where(userTable.keyNames, BinOp.EQ).buildAs[Username, Int],
    userTable.select.count.where('lastName, BinOp.EQ).buildAs[String, Int]
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
