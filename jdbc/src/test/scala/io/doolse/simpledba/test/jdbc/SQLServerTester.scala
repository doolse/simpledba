package io.doolse.simpledba.test.jdbc

import java.sql.DriverManager

import fs2.Stream
import io.doolse.simpledba.jdbc.sqlserver._

object SQLServerTester extends App with JDBCTester[SQLServerColumn] with StdSQLServerColumns {

  lazy val connection = DriverManager.getConnection(
    "jdbc:sqlserver://localhost:1433;database=simpledba;",
    "sa",
    "yourStrong(!)Password")

  def mapper           = sqlServerMapper
  val sqlServerQueries = new SQLServerQueries(mapper.dialect, effect)

  val q = makeQueries
  val prog = for {
    t <- q.initDB
    r <- doTest(q, (o, n) => n.copy(o.uniqueid))
  } yield r

  println(prog.unsafeRunSync())

  override def insertInst = sqlServerQueries.insertIdentity(instTable)
}
