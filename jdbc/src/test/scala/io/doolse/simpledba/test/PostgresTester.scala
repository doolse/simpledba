package io.doolse.simpledba.test

import java.sql.DriverManager

import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.jdbc.postgres._
import zio.stream.ZSink
import zio.{App, ZIO}

object PostgresTester extends App with JDBCZIOTester[PostgresColumn] with StdPostgresColumns {

  lazy val connection = DriverManager.getConnection("jdbc:postgresql:simpledba2", "equellauser", "tle010")

  def mapper = postgresMapper
  val seq = Sequence[Long]("uniquevals")
  val postgresQueries = new PostgresQueries(mapper.dialect, effect)

  val q = makeQueries
  val prog = for {
    t <- S.eval(q.initDB)
    r <- doTest(q)
  } yield r

  override def run(args: List[String]): ZIO[PostgresTester.Environment, Nothing, Int] = {
    prog.run(ZSink.identity[String].?).fold(_ => 1, v => {println(v); 0})
  }


  override def insertInst =
    postgresQueries.insertWith(instTable, seq)
}
