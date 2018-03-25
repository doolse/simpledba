package io.doolse.simpledba.test

import java.sql.DriverManager

import cats.effect.IO
import io.doolse.simpledba.test.Test._
import io.doolse.simpledba2.jdbc._
import io.doolse.simpledba2.syntax._
import shapeless.syntax.singleton._
import shapeless._
import fs2.Stream

object Tester extends App {

  val connection = DriverManager.getConnection("jdbc:postgresql:simpledba2", "equellauser", "tle010")

  implicit val config = PostgresMapper.postgresConfig
  implicit val cols = TableMapper[EmbeddedFields].embedded
  val instTable = TableMapper[Inst].table("inst").primaryKey('uniqueid)
  val userTable = TableMapper[User].table("user").primaryKey('firstName, 'lastName)


  val q = Queries(instTable.writes, userTable.writes,
    instTable.query.whereEQ(instTable.keys).build[Long],
    userTable.query.whereEQ(userTable.col('firstName)).orderWith(HList('lastName ->> false,
      'year ->> false)).build[String],
    userTable.query.whereEQ(userTable.col('lastName)).orderBy('year, true).build[String],
    userTable.query.whereEQ(userTable.keys).build[Username])

  val prog = for {
    _ <- Stream(JDBCTruncate(instTable.name),
        JDBCTruncate(userTable.name)).map(c =>
        JDBCWriteOp(c, config, (_,_) => IO.pure())).covary[JDBCIO].flush
    r <- Test.doTest(q)
  } yield r

  println(prog.compile.last.runA(connection).unsafeRunSync())


}
