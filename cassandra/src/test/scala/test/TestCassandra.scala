package test

import com.datastax.driver.core.schemabuilder.SchemaBuilder
import fs2.interop.cats._
import io.doolse.simpledba.cassandra._

/**
  * Created by jolz on 5/05/16.
  */
object TestCassandra extends App {
  val mapper = new CassandraMapper()

  val built = mapper.buildModel(TestCreator.model)
  val queries = built.queries

  val session = CassandraSession.simpleSession("localhost")
  val sessionConfig = SessionConfig(session, s => println(s()))
  CassandraUtils.initKeyspaceAndSchema(sessionConfig, "test", built.ddl, dropKeyspace = true).unsafeRun

  val q = TestCreator.doTest(queries)
  val res = q.run(sessionConfig).unsafeRun
  println(res)
}
