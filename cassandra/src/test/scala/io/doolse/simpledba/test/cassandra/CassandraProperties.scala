package io.doolse.simpledba.test.cassandra

import com.datastax.driver.core.schemabuilder.Create
import io.doolse.simpledba.BuiltQueries
import io.doolse.simpledba.cassandra._
import io.doolse.simpledba.cassandra.{CassandraMapper, CassandraIO, CassandraUtils, CassandraSession}

/**
  * Created by jolz on 16/06/16.
  */

object CassandraProperties {
  lazy val sessionConfig = CassandraSession(CassandraIO.initSimpleSession())
  lazy val initKS = CassandraUtils.initKeyspaceAndSchema(sessionConfig, "test", List.empty, dropKeyspace = true).unsafeRun
}

trait CassandraProperties {
  import CassandraProperties._

  lazy val mapper = new CassandraMapper()

  def setup[Q](bq: BuiltQueries.Aux[Q, (String, Create)]) = {
    initKS
    CassandraUtils.createSchema(sessionConfig.copy(logger = msg => Console.out.println(msg())), bq.ddl).unsafeRun
    bq.queries
  }

  def run[A](fa: Effect[A]): A = scala.concurrent.blocking { fa.run(sessionConfig).unsafeRun }

}
