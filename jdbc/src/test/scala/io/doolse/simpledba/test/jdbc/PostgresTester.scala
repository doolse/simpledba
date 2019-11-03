package io.doolse.simpledba.test.jdbc

import java.sql.DriverManager

import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.jdbc.postgres._
import zio.stream.ZSink
import zio.{App, Task, ZIO}

object PostgresTester extends App with JDBCZIOTester with StdPostgresColumns {

  lazy val connection = DriverManager.getConnection("jdbc:postgresql:simpledba2", "equellauser", "tle010")

  def mapper = postgresMapper
  val seq = Sequence[Long]("uniquevals")
  val postgresQueries = new PostgresQueries(mapper.dialect, effect)

  val q = makeQueries
  val prog = for {
    t <- q.initDB
    r <- doTest(q)
  } yield r

  override def run(args: List[String]) = {
    prog.fold(_ => 1, v => {println(v); 0})
  }

  override def insertInst : (Long => Inst) => Task[Inst] =
    postgresQueries.insertWith(instTable, seq)
}
