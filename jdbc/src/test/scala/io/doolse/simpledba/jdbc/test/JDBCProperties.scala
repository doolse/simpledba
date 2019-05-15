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

  implicit def shortCol = HSQLColumn[Short](StdJDBCColumn.shortCol, ColumnType("INTEGER"))

  lazy val mapper        = hsqldbMapper
  def effect             = StateIOEffect()
  lazy val sqlQueries    = mapper.queries(effect)
  implicit def flushable = effect.flushable

  import sqlQueries._
  def setup(bq: JDBCTable*): Unit =
    run((for {
      t <- Stream.emits(bq).map(_.definition)
      _ <- rawSQLStream(Stream(dialect.dropTable(t), dialect.createTable(t))).flush
    } yield ()).compile.drain)

  def run[A](fa: JDBCIO[A]): A = scala.concurrent.blocking { fa.runA(connection).unsafeRunSync() }

}
