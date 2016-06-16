package io.doolse.simpledba.test.cassandra

import com.datastax.driver.core.schemabuilder.Create
import io.doolse.simpledba.BuiltQueries
import io.doolse.simpledba.cassandra.CassandraMapper._
import io.doolse.simpledba.cassandra.{CassandraMapper, CassandraSession, CassandraUtils, SessionConfig}

/**
  * Created by jolz on 16/06/16.
  */
trait CassandraProperties {

  lazy val sessionConfig = SessionConfig(CassandraSession.initSimpleSession())
  lazy val initKS = CassandraUtils.initKeyspaceAndSchema(sessionConfig, "test", List.empty, dropKeyspace = true).unsafeRun

  lazy val mapper = new CassandraMapper()

  def setup[Q](bq: BuiltQueries.Aux[Q, (String, Create)]) = {
    initKS
    CassandraUtils.createSchema(sessionConfig, bq.ddl).unsafeRun
    bq.queries
  }

  def run[A](fa: Effect[A]): A = fa.run(sessionConfig).unsafeRun

}
