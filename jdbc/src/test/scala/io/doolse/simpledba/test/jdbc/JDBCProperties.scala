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
    if (testConfig.getBoolean("log")) new PrintLnLogger()
    else new NothingLogger()
  }
}

trait JDBCProperties[S[_], F[_]] {

  import JDBCProperties._

  implicit def shortCol = HSQLColumn[Short](StdJDBCColumn.shortCol, ColumnType("INTEGER"))

  implicit def streamable: Streamable[S, F]

  def flush(w: S[JDBCWriteOp]): F[Unit] = sqlQueries.flush(w)

  implicit def JE: JavaEffects[F]

  implicit def Sync: Sync[F]

  lazy val mapper = hsqldbMapper

  def effect = JDBCEffect[S, F](streamable.M.pure(connection), _ => streamable.M.pure(), mkLogger)

  lazy val sqlQueries = mapper.queries(effect)

  def setup(bq: JDBCTable[HSQLColumn]*): Unit = {
    implicit val SM = streamable.SM
    import sqlQueries.{flush => _, _}
    run {
      flush {
        SM.flatMap(streamable.emits(Seq(bq: _*)))(dropAndCreate)
      }
    }
  }

  def run[A](fa: F[A]): A
}
