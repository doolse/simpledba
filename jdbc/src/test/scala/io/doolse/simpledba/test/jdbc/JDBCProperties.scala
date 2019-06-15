package io.doolse.simpledba.test.jdbc

import cats.effect.Sync
import io.doolse.simpledba.{JavaEffects, Streamable}
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.jdbc.hsql._
import io.doolse.simpledba.syntax._
import cats.syntax.functor._
import cats.syntax.flatMap._
import com.typesafe.config.ConfigFactory

object JDBCProperties {
  val config          = ConfigFactory.load()
  lazy val connection = connectionFromConfig(config)
  def mkLogger[F[_]: Sync]: JDBCLogger[F] = {
    val testConfig = config.getConfig("simpledba.test")
    if (testConfig.getBoolean("log")) {
      new ConsoleLogger()
    } else new NothingLogger()
  }
}

trait JDBCProperties[S[_], F[_]] {
  import JDBCProperties._

  implicit def shortCol = HSQLColumn[Short](StdJDBCColumn.shortCol, ColumnType("INTEGER"))

  implicit def S: Streamable[S, F]
  implicit def JE: JavaEffects[F]
  implicit def Sync: Sync[F]

  lazy val mapper = hsqldbMapper
  def effect      = JDBCEffect[S, F](S.M.pure(connection), _ => S.M.pure(), mkLogger)

  lazy val sqlQueries    = mapper.queries(effect)
  implicit def flushable = sqlQueries.flushable

  import sqlQueries._

  def setup(bq: JDBCTable[HSQLColumn]*): Unit = {
    implicit val SM = S.SM
    run(S.drain {
      for {
        t <- S.emits(Seq(bq: _*)).map(_.definition)
        _ <- flushable.flush(
          rawSQLStream(S.emits(Seq(dialect.dropTable(t), dialect.createTable(t)))))
      } yield ()
    })
  }

  def run[A](fa: F[A]): A

}
