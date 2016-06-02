package test

import com.datastax.driver.core.schemabuilder.{Create, SchemaBuilder}
import io.doolse.simpledba.cassandra._
import shapeless._
import fs2.interop.cats._
import io.doolse.simpledba.RelationModel
import shapeless.ops.hlist.{ConstMapper, RightFolder, Transposer}

import scala.util.{Failure, Try}

/**
  * Created by jolz on 5/05/16.
  */
object TestCassandra extends App {
  val mapper = new CassandraMapper()
  import mapper._

  val built = mapper.buildModel(TestCreator.model)

  val session = CassandraSession.simpleSession("localhost", Some("eps"))
//  println(test.showType(built))
  val queries = built.as[TestCreator.Queries]()
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
