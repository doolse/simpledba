package io.doolse.simpledba.jdbc.test

import fs2._
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.jdbc.hsql._
import io.doolse.simpledba.syntax._
import shapeless.HList

object JDBCProperties
{
  lazy val connection = connectionFromConfig()
}

trait JDBCProperties {
  import JDBCProperties._

  implicit lazy val config = hsqldbConfig.withBindingLogger(msg => println(msg()))

  def setup[C[_] <: JDBCColumn](bq: JDBCTable[C, _, _  <: HList, _ <: HList]*) : Unit = run(
    (for {
      t <- Stream.emits(bq).map(_.definition)
      _ <- (dropTable(t) ++ createTable(t)).flush
    } yield ()).compile.drain)

  def run[A](fa: JDBCIO[A]): A = scala.concurrent.blocking { fa.runA(connection).unsafeRunSync() }

}
