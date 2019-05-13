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

  implicit lazy val config = hsqldbConfig.withBindingLogger((sql,vals) => println(s"$sql $vals"))
  lazy val schemaSql = config.schemaSQL

  def setup(bq: JDBCTable*) : Unit = run(
    (for {
      t <- Stream.emits(bq).map(_.definition)
      _ <- rawSQLStream[JDBCIO2](Stream(schemaSql.dropTable(t), schemaSql.createTable(t))).flush
    } yield ()).compile.drain)

  def run[A](fa: JDBCIO2[A]): A = scala.concurrent.blocking { fa.runA(connection).unsafeRunSync() }

}
