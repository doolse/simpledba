package io.doolse.simpledba.test.jdbc

import io.doolse.simpledba.BuiltQueries
import io.doolse.simpledba.jdbc._

/**
  * Created by jolz on 16/06/16.
  */

object JDBCProperties {
  lazy val session =JDBCIO.openConnection() // .copy(logger = msg => Console.out.println(msg()))
}

trait JDBCProperties {
  import JDBCProperties._

  lazy val mapper = new JDBCMapper()

  def setup[Q](bq: BuiltQueries.Aux[Q, JDBCCreateTable]) = {
    JDBCUtils.createSchema(session.copy(logger = msg => Console.out.println(msg())), bq.ddl, drop = true).unsafeRun
    bq.queries
  }

  def run[A](fa: Effect[A]): A = scala.concurrent.blocking { JDBCIO.runWrites(fa).run(session).unsafeRun }

}
