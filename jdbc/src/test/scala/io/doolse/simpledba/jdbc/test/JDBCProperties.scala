package io.doolse.simpledba.jdbc.test

import fs2._
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.jdbc.hsql._
import io.doolse.simpledba.syntax._

object JDBCProperties {
  lazy val connection = connectionFromConfig()
}

trait JDBCProperties {
  import JDBCProperties._

  lazy val mapper        = hsqldbMapper
  def effect             = StateIOEffect(ConsoleLogger())
  def dialect            = mapper.dialect
  lazy val sqlQueries    = mapper.queries(effect)
  implicit def flushable = flushJDBC(effect)

  def setup(bq: JDBCTable*): Unit =
    run((for {
      t <- Stream.emits(bq).map(_.definition)
      _ <- sqlQueries.rawSQLStream(Stream(dialect.dropTable(t), dialect.createTable(t))).flush
    } yield ()).compile.drain)

  def run[A](fa: JDBCIO[A]): A = scala.concurrent.blocking { fa.runA(connection).unsafeRunSync() }

}
