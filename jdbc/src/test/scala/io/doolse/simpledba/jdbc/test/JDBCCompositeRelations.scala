package io.doolse.simpledba.jdbc.test

import io.doolse.simpledba.jdbc._
import CompositeRelations.{Composite2, Composite3, Queries2, Queries3}

object JDBCCompositeRelations extends CompositeRelations[JDBCIO]("JDBC Composite") with JDBCProperties {

  lazy val compTable = TableMapper[Composite2].table("composite2").key('pkLong, 'pkUUID)
  lazy val compRel3 = TableMapper[Composite3].table("composite3").keys('pkInt, 'pkString, 'pkBool)


  lazy val queries2 = {
    setup(compTable)
    Queries2(compTable.writes, compTable.queryByPK)
  }

  lazy val queries3 = {
    setup(compRel3)
    Queries3(compRel3.writes, compRel3.queryByPK)
  }
}
