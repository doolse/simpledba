package io.doolse.simpledba.test

import cats.Monad
import cats.effect.IO
import io.doolse.simpledba.fs2._
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.jdbc.hsql._
import io.doolse.simpledba.syntax._
import fs2._

object JDBCProperties {
  lazy val connection = connectionFromConfig()
}

trait JDBCProperties {
  import JDBCProperties._

  implicit def shortCol = HSQLColumn[Short](StdJDBCColumn.shortCol, ColumnType("INTEGER"))

  lazy val mapper        = hsqldbMapper
  def effect             = JDBCEffect[fs2.Stream[IO, ?], IO](
    IO.pure(connection), _ => IO.pure(), new NothingLogger)
  def S = effect.S
  lazy val sqlQueries    = mapper.queries(effect)
  implicit def flushable = sqlQueries.flushable

  import sqlQueries._

  def setup(bq: JDBCTable*): Unit =
    run((for {
      t <- Stream.emits(bq).map(_.definition)
      _ <- rawSQLStream(Stream(dialect.dropTable(t), dialect.createTable(t))).flush
    } yield ()).compile.drain)

  def run[A](fa: IO[A]): A = scala.concurrent.blocking { fa.unsafeRunSync() }

}
