package io.doolse.simpledba.test.jdbc

import cats.effect.Sync
import io.doolse.simpledba.{IOEffects, JavaEffects, StreamEffects}
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.jdbc.hsql._
import io.doolse.simpledba.syntax._
import cats.syntax.functor._
import cats.syntax.flatMap._
import com.typesafe.config.ConfigFactory

object JDBCProperties {
  val config          = ConfigFactory.load()
  lazy val connection = connectionFromConfig(config)

  def mkLogger[F[-_, _]](implicit S: IOEffects[F]): JDBCLogger[F, Any] = {
    val testConfig = config.getConfig("simpledba.test")
    if (testConfig.getBoolean("log")) PrintLnLogger[F]()
    else NothingLogger()
  }
}

trait JDBCProperties[S[-_, _], F[-_, _]] {

  import JDBCProperties._

  implicit def shortCol = HSQLColumn[Short](StdJDBCColumn.shortCol, ColumnType("INTEGER"))

  def streamable: StreamEffects[S, F]
  implicit def javaEffects : JavaEffects[F]

  def flush(w: S[Any, JDBCWriteOp]): F[Any, Unit] = sqlQueries.flush(w)

  lazy val mapper = hsqldbMapper

  def effect: JDBCEffect[S, F, Any] = {
    implicit val ioEffects : IOEffects[F] = streamable
    JDBCEffect.withLogger(streamable, providedJDBCConnection(connection), mkLogger)
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

  def run[A](fa: F[Any, A]): A
}
