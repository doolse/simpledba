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

  val init = CassandraRelationIO.initialiseSession(CassandraSession.simpleSession("localhost", Some("eps")))
  println(TestQuery.doQueryWithTable(mapper)(pt, HList(517573426L)).run(init).unsafeRun)
}
