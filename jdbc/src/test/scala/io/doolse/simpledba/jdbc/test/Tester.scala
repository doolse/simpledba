package io.doolse.simpledba.jdbc.test

import java.sql.DriverManager

import fs2.Stream
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.syntax._
import Test._
import shapeless._
import shapeless.syntax.singleton._

object Tester extends App {

  val connection = DriverManager.getConnection("jdbc:postgresql:simpledba2", "equellauser", "tle010")

  implicit val config = Dialects.postgresConfig
  implicit val cols = TableMapper[EmbeddedFields].embedded
  val instTable = TableMapper[Inst].table("inst").key('uniqueid)
  val userTable = TableMapper[User].table("user").key('firstName, 'lastName)


  val q = Queries(instTable.writes, userTable.writes,
    instTable.query.whereEQ(instTable.keys).build[Long],
    userTable.query.whereEQ(userTable.col('firstName)).orderWith(HList('lastName ->> false,
      'year ->> false)).build[String],
    userTable.query.whereEQ(userTable.col('lastName)).orderBy('year, true).build[String],
    userTable.query.whereEQ(userTable.keys).build[Username])

  val prog = for {
    _ <- Stream(instTable.dropTable, instTable.createTable,
        userTable.dropTable, userTable.createTable).covary[JDBCIO].flush
    r <- Test.doTest(q)
  } yield r

  println(prog.compile.last.runA(connection).unsafeRunSync())


}
