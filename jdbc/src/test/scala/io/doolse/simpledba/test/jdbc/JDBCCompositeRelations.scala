package io.doolse.simpledba.test.jdbc

import io.doolse.simpledba.Cols
import io.doolse.simpledba.jdbc.JDBCWriteOp
import io.doolse.simpledba.test.CompositeRelations
import io.doolse.simpledba.test.CompositeRelations.{Composite2, Composite3}
import io.doolse.simpledba.test.zio.ZIOProperties
import zio.Task
import zio.interop.catz._
import zio.stream.ZStream

object JDBCCompositeRelations
    extends CompositeRelations[ZStream[Any, Throwable, ?], Task, JDBCWriteOp]("JDBC Composite")
    with JDBCProperties[ZStream[Any, Throwable, ?], Task] with ZIOProperties {

  import mapper.mapped

  lazy val compTable = mapped[Composite2].table("composite2").keys(Cols('pkLong, 'pkUUID))
  lazy val compRel3  = mapped[Composite3].table("composite3").keys(Cols('pkInt, 'pkString, 'pkBool))

  import sqlQueries._
  lazy val queries2 = {
    setup(compTable)
    Queries2(writes(compTable),
             byPK(compTable).build,
             rawSQLStream(streamable.emit(dialect.truncateTable(compTable.definition))))
  }

  lazy val queries3 = {
    setup(compRel3)
    Queries3(writes(compRel3),
             byPK(compRel3).build,
             rawSQLStream(streamable.emit(dialect.truncateTable(compRel3.definition))))
  }
}
