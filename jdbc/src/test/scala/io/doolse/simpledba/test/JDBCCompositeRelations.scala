package io.doolse.simpledba.test

import fs2.Stream
import io.doolse.simpledba.Cols
import io.doolse.simpledba.jdbc._
import CompositeRelations.{Composite2, Composite3, Queries2, Queries3}

object JDBCCompositeRelations extends CompositeRelations[fs2.Stream, JDBCIO]("JDBC Composite") with JDBCProperties {

  import mapper.mapped

  lazy val compTable = mapped[Composite2].table("composite2").keys(Cols('pkLong, 'pkUUID))
  lazy val compRel3 = mapped[Composite3].table("composite3").keys(Cols('pkInt, 'pkString, 'pkBool))

  import sqlQueries._
  lazy val queries2 = {
    setup(compTable)
    Queries2(writes(compTable), byPK(compTable),
      rawSQLStream(Stream.emit(dialect.truncateTable(compTable.definition))))
  }

  lazy val queries3  = {
    setup(compRel3)
    Queries3(writes(compRel3), byPK(compRel3),
      rawSQLStream(Stream.emit(dialect.truncateTable(compRel3.definition))))
  }
}
