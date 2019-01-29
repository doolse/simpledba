package io.doolse.simpledba.jdbc.test

import io.doolse.simpledba.jdbc._
import CompositeRelations.{Composite2, Composite3, Queries2, Queries3}
import fs2.Stream
import io.doolse.simpledba.Cols

object JDBCCompositeRelations extends CompositeRelations[JDBCIO]("JDBC Composite") with JDBCProperties {

  lazy val compTable = TableMapper[Composite2].table("composite2").keys(Cols('pkLong, 'pkUUID))
  lazy val compRel3 = TableMapper[Composite3].table("composite3").keys(Cols('pkInt, 'pkString, 'pkBool))


  lazy val queries2 = {
    setup(compTable)
    Queries2(compTable.writes, compTable.byPK,
      rawSQLStream(Stream.emit(config.schemaSQL.truncateTable(compTable.definition))))
  }

  lazy val queries3 : Queries3[JDBCIO] = {
    setup(compRel3)
    Queries3(compRel3.writes, compRel3.byPK,
      rawSQLStream(Stream.emit(config.schemaSQL.truncateTable(compRel3.definition))))
  }
}
