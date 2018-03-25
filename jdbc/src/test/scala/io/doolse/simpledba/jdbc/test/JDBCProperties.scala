package io.doolse.simpledba.jdbc.test

import java.sql.DriverManager

import fs2._
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.syntax._
import shapeless.HList

object JDBCProperties
{
  lazy val connection = DriverManager.getConnection("jdbc:postgresql:simpledba2",
    "equellauser", "tle010")


}

trait JDBCProperties {
  import JDBCProperties._

  implicit lazy val config = PostgresMapper.postgresConfig

  def setup[C[_] <: JDBCColumn](bq: JDBCTable[C, _, _  <: HList, _ <: HList]*) : Unit = run(
    (for {
      t <- Stream.emits(bq)
      _ <- Stream(t.dropTable, t.createTable).covary[JDBCIO].flush
    } yield ()).compile.drain)

  def run[A](fa: JDBCIO[A]): A = scala.concurrent.blocking { fa.runA(connection).unsafeRunSync() }

}
