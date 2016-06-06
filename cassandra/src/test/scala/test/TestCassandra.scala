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

  val session = CassandraSession.simpleSession("localhost", Some("eps"))
  val creation = built.ddl.value
  creation.foreach {
    case (name, c) => {
      session.execute(SchemaBuilder.dropTable(name).ifExists())
      println(c.getQueryString())
      session.execute(c)
    }
  }

  val q = TestCreator.doTest(queries)
  val res = q.run(SessionConfig(session, s => println(s()))).unsafeRun
  println(res)

}
