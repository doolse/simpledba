package io.doolse.simpledba.test.cassandra

import com.datastax.driver.core.schemabuilder.Create
import io.doolse.simpledba.BuiltQueries
import io.doolse.simpledba.cassandra.CassandraMapper.Effect
import io.doolse.simpledba.cassandra.{CassandraMapper, CassandraSession, CassandraUtils, SessionConfig}
import io.doolse.simpledba.test.RelationShapes
import io.doolse.simpledba.test.RelationShapes._
import fs2.interop.cats._

/**
  * Created by jolz on 15/06/16.
  */
object CassandraRelationShapes extends RelationShapes[Effect]("DynamoDB") {

  lazy val sessionConfig = SessionConfig(CassandraSession.simpleSession("localhost"))
  lazy val initKS = CassandraUtils.initKeyspaceAndSchema(sessionConfig, "test", List.empty, dropKeyspace = true).unsafeRun

  lazy val mapper = new CassandraMapper()

  def setup[Q](bq: BuiltQueries.Aux[Q, (String, Create)]) = {
    initKS
    CassandraUtils.createSchema(sessionConfig, bq.ddl).unsafeRun
    bq.queries
  }

  def run[A](fa: Effect[A]): A = fa.run(sessionConfig).unsafeRun

  lazy val gen1: Fields1Queries[Effect] = {
    setup(mapper.buildModel(fields1Model))
  }

  lazy val gen2: Fields2Queries[Effect] = {
    setup(mapper.buildModel(fields2Model))
  }

  lazy val gen3: Fields3Queries[Effect] = {
    setup(mapper.buildModel(fields3Model))
  }
}
