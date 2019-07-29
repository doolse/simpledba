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

  def mkLogger[S[-_, _], F[-_, _]](implicit S: Streamable[S, F]): JDBCLogger[F, Any] = {
    val testConfig = config.getConfig("simpledba.test")
    if (testConfig.getBoolean("log")) PrintLnLogger[S, F]()
    else NothingLogger[S, F]()
  }
}

trait JDBCProperties[S[-_, _], F[-_, _]] {

  import JDBCProperties._

  implicit def shortCol = HSQLColumn[Short](StdJDBCColumn.shortCol, ColumnType("INTEGER"))

  implicit def streamable: Streamable[S, F]
  implicit def javaEffects : JavaEffects[F]

  def flush(w: S[Any, JDBCWriteOp]): F[Any, Unit] = sqlQueries.flush(w)

  lazy val mapper = hsqldbMapper

  def effect = JDBCEffect[S, F, Any](singleJDBCConnection(connection), mkLogger)

  lazy val sqlQueries = mapper.queries(effect)

  def setup(bq: JDBCTable[HSQLColumn]*): Unit = {
    implicit val S = streamable
    import sqlQueries.{flush => _, _}
    run {
      flush {
        S.flatMapS(streamable.emits(Seq(bq: _*)))(dropAndCreate)
      }
    }
  }

  def run[A](fa: F[Any, A]): A
}
