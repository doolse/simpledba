package test

import com.datastax.driver.core.schemabuilder.{Create, SchemaBuilder}
import io.doolse.simpledba.cassandra._
import shapeless._
import fs2.interop.cats._
import io.doolse.simpledba.{RelationModel, VerifierContext}
import shapeless.labelled.FieldType
import shapeless.ops.hlist.{ConstMapper, RightFolder, Transposer}
import RealisticModel.InstitutionId

import scala.util.{Failure, Try}

/**
  * Created by jolz on 5/05/16.
  */
object TestCassandra extends App {
  val mapper = new CassandraMapper()
  import mapper._

  val built = mapper.verifyModel(RealisticModel.model, Console.err.println)
//  val built = mapper.buildModel(RealisticModel.model)
  val queries : RealisticModel.Queries = ??? //built.as()

  val session = CassandraSession.simpleSession("localhost", Some("eps"))
  val creation = built.ddl.value
  creation.foreach {
    case (name, c) => {
      session.execute(SchemaBuilder.dropTable(name).ifExists())
      println(c.getQueryString())
      session.execute(c)
    }
  }

//  val q = TestCreator.doTest(queries)
//  val res = q.run(SessionConfig(session, s => println(s()))).unsafeRun
//  println(res)

}
