package io.doolse.simpledba.jdbc.test

import fs2.Stream
import io.doolse.simpledba.Cols
import io.doolse.simpledba.jdbc._
import io.doolse.simpledba.jdbc.test.CompositeRelations.{Composite2, Composite3, Queries2, Queries3}

object JDBCCompositeRelations extends CompositeRelations[JDBCIO2]("JDBC Composite") with JDBCProperties {

  lazy val compTable = TableMapper[Composite2].table("composite2").keys(Cols('pkLong, 'pkUUID))
  lazy val compRel3 = TableMapper[Composite3].table("composite3").keys(Cols('pkInt, 'pkString, 'pkBool))
  lazy val qb = new JDBCQueries[JDBCIO2]
  import qb._

  lazy val queries2 = {
    setup(compTable)
    Queries2(writes(compTable), byPK(compTable),
      rawSQLStream(Stream.emit(config.schemaSQL.truncateTable(compTable.definition))))
  }

  lazy val queries3 : Queries3[JDBCIO2] = {
    setup(compRel3)
    Queries3(writes(compRel3), byPK(compRel3),
      rawSQLStream(Stream.emit(config.schemaSQL.truncateTable(compRel3.definition))))
  }
}
