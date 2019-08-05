package io.doolse.simpledba.test.jdbc

import io.doolse.simpledba.Cols
import io.doolse.simpledba.interop.zio.ZStreamR
import io.doolse.simpledba.jdbc.JDBCWriteOp
import io.doolse.simpledba.test.CompositeRelations
import io.doolse.simpledba.test.CompositeRelations.{Composite2, Composite3}
import io.doolse.simpledba.test.zio.ZIOProperties
import zio.RIO
import zio.interop.catz._

object JDBCCompositeRelations
    extends CompositeRelations[ZStreamR, RIO, JDBCWriteOp]("JDBC Composite")
    with JDBCProperties[ZStreamR, RIO] with ZIOProperties {

  import mapper.mapped

  lazy val compTable = mapped[Composite2].table("composite2").keys(Cols('pkLong, 'pkUUID))
  lazy val compRel3  = mapped[Composite3].table("composite3").keys(Cols('pkInt, 'pkString, 'pkBool))

  import sqlQueries._
  lazy val queries2 = {
    setup(compTable)
    Queries2(writes(compTable),
             byPK(compTable).build,
             streamable.emit(sql(dialect.truncateTable(compTable.definition))))
  }

  lazy val queries3 = {
    setup(compRel3)
    Queries3(writes(compRel3),
             byPK(compRel3).build,
             streamable.emit(sql(dialect.truncateTable(compRel3.definition))))
  }
}
