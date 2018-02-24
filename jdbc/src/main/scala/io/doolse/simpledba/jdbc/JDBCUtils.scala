package io.doolse.simpledba.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement, SQLType}
import java.util
import java.util.concurrent.ConcurrentHashMap

import cats.data.StateT
import cats.effect.IO
import cats.syntax.traverse._
import cats.instances.vector._
import cats.syntax.apply._
import cats.~>
import com.typesafe.config.{Config, ConfigFactory}
import javax.sql.DataSource
import fs2.Stream
import scala.collection.JavaConverters._

/**
  * Created by jolz on 12/03/17.
  */
object JDBCUtils {

  def brackets(c: Iterable[String]): String = c.mkString("(", ",", ")")


  def createSchema(tables: Iterable[JDBCCreateTable], drop: Boolean): Effect[Unit] = JDBCIO.sessionIO { session =>
    tables.toVector.traverse { t =>
      (if (drop) session.execWrite(JDBCDropTable(t.name), Seq.empty)
      else IO.pure(false)) *>
        session.execWrite(t, Seq.empty)
    }.map(_ => ())
  }


  def sessionFromConfig(config: Config = ConfigFactory.load()): JDBCSession = {
    val jdbcConfig = config.getConfig("simpledba.jdbc")
    val logger = if (!jdbcConfig.hasPath("log")) None else Some((msg: () => String) => Console.out.println(msg()))
    val jdbcUrl = jdbcConfig.getString("url")
    val dialect = jdbcConfig.getString("dialect") match {
      case "hsqldb" => JDBCSQLConfig.hsqldbConfig
      case "postgres" => JDBCSQLConfig.postgresConfig
      case "sqlserver" => JDBCSQLConfig.sqlServerConfig
      case "oracle" => JDBCSQLConfig.oracleConfig
    }
    val con = if (jdbcConfig.hasPath("credentials")) {
      val cc = jdbcConfig.getConfig("credentials")
      DriverManager.getConnection(jdbcUrl, cc.getString("username"), cc.getString("password"))
    } else DriverManager.getConnection(jdbcUrl)
    JDBCSession(con, dialect, logger = logger.getOrElse(_ => ()))
  }

  def usingDataSource(acquireConnection: IO[Connection], sqlConfig: JDBCSQLConfig, logger: (() => String) => Unit,
                      statementCache: Connection => scala.collection.concurrent.Map[Any, IO[PreparedStatement]]): Stream[Effect, ?] ~> Stream[IO, ?] = {
    new (Stream[Effect, ?] ~> Stream[IO, ?]) {
      def apply[A](s: Stream[Effect, A]): Stream[IO, A] =
        Stream.bracket(acquireConnection)({
          con =>
            val session = JDBCSession(con, JDBCSQLConfig.postgresConfig, logger,
              statementCache(con))
            s.translate(new (Effect[?] ~> IO[?]) {
              def apply[A](ef: Effect[A]): IO[A] = ef.runA(session)
            })
        }, con => IO(con.close()))
    }
  }

}
