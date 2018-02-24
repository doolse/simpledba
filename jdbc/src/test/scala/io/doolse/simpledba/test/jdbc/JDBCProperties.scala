package io.doolse.simpledba.test.jdbc

import io.doolse.simpledba.BuiltQueries
import io.doolse.simpledba.jdbc._

/**
  * Created by jolz on 16/06/16.
  */

object JDBCProperties {
  lazy val session =JDBCUtils.sessionFromConfig() // .copy(logger = msg => Console.out.println(msg()))
}

trait JDBCProperties {
  import JDBCProperties._

  lazy val mapper = new JDBCMapper()

  def setup[Q](bq: BuiltQueries.Aux[Q, JDBCCreateTable]) = {
    JDBCUtils.createSchema(bq.ddl, drop = true)
      .runA(session.copy(logger = msg => Console.out.println(msg())))
      .unsafeRunSync()
    bq.queries
  }

  def run[A](fa: Effect[A]): A = scala.concurrent.blocking { fa.runA(session).unsafeRunSync() }

}
