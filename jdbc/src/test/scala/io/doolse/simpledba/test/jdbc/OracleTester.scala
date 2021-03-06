package io.doolse.simpledba.test.jdbc

import java.sql.DriverManager

import fs2.Stream
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.jdbc.oracle._

object OracleTester extends App with JDBCTester with StdOracleColumns {

  lazy val connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:OraDoc",
                                               "simpledba",
                                               "simpledba123")

  def mapper           = oracleMapper
  val seq = Sequence[Long]("ids")
  val oracleQueries = new OracleQueries(mapper.dialect, effect)

  val q = makeQueries

  val prog = for {
    t <- q.initDB
    r <- doTest(q)
  } yield r

  println(prog.unsafeRunSync())

  override def insertInst =
    oracleQueries.insertWith(instTable, seq)
}
