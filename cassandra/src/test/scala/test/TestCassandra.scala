package test

import io.doolse.simpledba.cassandra._
import shapeless.HList
import fs2.interop.cats._

/**
  * Created by jolz on 5/05/16.
  */
object TestCassandra extends App {

  val cassdb = CassandraRelationIO()

  case class Inst(uniqueid: Long, adminpassword: String, enabled: Boolean)

  val mapper = new CassandraMapper()

  import mapper._

  val table = mapper.table[Inst]("institution").key('uniqueid)
  val pt = mapper.physicalTable(table)
  val queries = mapper.queries(pt).key[Long]

  val init = CassandraRelationIO.initialiseSession(CassandraSession.simpleSession("localhost", Some("eps")))
  val q = TestQuery.insertAndQuery(queries, Inst(517573426L, "ok", true), 517573426L)
  println(q.run(init).unsafeRun)
}
