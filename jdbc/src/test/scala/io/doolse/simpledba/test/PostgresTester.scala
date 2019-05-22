package io.doolse.simpledba.test

import java.sql.DriverManager

import fs2.Stream
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.jdbc.postgres._

object PostgresTester extends App with JDBCTester[PostgresColumn] with StdPostgresColumns {

  val connection = DriverManager.getConnection("jdbc:postgresql:simpledba2", "equellauser", "tle010")

  def mapper = postgresMapper
  val seq = Sequence[Long]("uniquevals")
  val postgresQueries = new PostgresQueries(mapper.dialect, effect)

  val q = makeQueries
  val prog = for {
    t <- Stream.eval(q.initDB)
    r <- doTest(q)
  } yield r

  println(prog.compile.last.runA(connection).unsafeRunSync())

  override def insertInst =
    postgresQueries.insertWith(instTable, seq)
}
