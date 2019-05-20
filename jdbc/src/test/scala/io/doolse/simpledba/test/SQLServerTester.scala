package io.doolse.simpledba.test

import java.sql.DriverManager

import fs2.Stream
import io.doolse.simpledba.jdbc.sqlserver._

object SQLServerTester extends App with JDBCTester[SQLServerColumn] with StdSQLServerColumns {

  val connection = DriverManager.getConnection(
    "jdbc:sqlserver://localhost:1433;database=simpledba;",
    "sa",
    "yourStrong(!)Password")

  def mapper           = sqlServerMapper
  val sqlServerQueries = new SQLServerQueries(mapper.dialect, effect)

  val q = makeQueries
  val prog = for {
    t <- Stream.eval(q.initDB)
    r <- Test.doTest(q, (o, n) => n.copy(o.uniqueid))
  } yield r

  println(prog.compile.last.runA(connection).unsafeRunSync())

  override def insertInst = sqlServerQueries.insertIdentity(instTable)
}
