package test

import io.doolse.simpledba.cassandra._
import shapeless.{HList, HNil, Poly2}
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

  import RelationModel._

 val built = mapper.buildModel(TestCreator.model)

  println(built)
//  val queries = built.as[TestCreator.Queries]()
//  val creation = built.ddl.value

//  val q = TestCreator.doTest(queries)
//  val res = q.run(SessionConfig(CassandraSession.simpleSession("localhost", Some("eps"))))
//  println(res)

}
