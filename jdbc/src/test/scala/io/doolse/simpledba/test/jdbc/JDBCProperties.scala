package io.doolse.simpledba.test.jdbc

import cats.effect.Sync
import io.doolse.simpledba.{JavaEffects, Streamable}
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.jdbc.hsql._
import com.typesafe.config.ConfigFactory

object JDBCProperties {
  val config          = ConfigFactory.load()
  lazy val connection = connectionFromConfig(config)

  def mkLogger[F[_]](implicit S: Sync[F]): JDBCLogger[F] = {
    val testConfig = config.getConfig("simpledba.test")
    if (testConfig.getBoolean("log")) PrintLnLogger[F]()
    else NothingLogger()
  }
}

trait JDBCProperties[S[_], F[_]] {

  import JDBCProperties._

  implicit def shortCol = HSQLColumn[Short](StdJDBCColumn.shortCol, ColumnType("INTEGER"))

  implicit def javaEffects : JavaEffects[F]

  def sync: Sync[F]
  def streamable: Streamable[S, F]

  def flush(w: S[JDBCWriteOp]): F[Unit] = sqlQueries.flush(w)

  lazy val mapper = hsqldbMapper

  def effect: JDBCEffect[S, F] = {
    implicit val S = streamable
    implicit val F = sync
    JDBCEffect.withLogger[S, F](providedJDBCConnection(connection), mkLogger[F])
  }

  lazy val sqlQueries = mapper.queries(effect)

  def setup(bq: JDBCTable[HSQLColumn]*): Unit = {
    implicit val S = streamable
    import sqlQueries.{flush => _, _}
    run {
      flush {
        S.flatMapS(streamable.emits(Seq(bq: _*)))(ddl.dropAndCreate)
      }
    }
  }

  def run[A](fa: F[A]): A
}
