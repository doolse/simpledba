package io.doolse.simpledba.test

import io.doolse.simpledba.Cols
import io.doolse.simpledba.test.CompositeRelations.{Composite2, Composite3, Queries2, Queries3}
import scalaz.zio.Task
import scalaz.zio.interop.catz._
import scalaz.zio.stream.ZStream

object JDBCCompositeRelations
    extends CompositeRelations[ZStream[Any, Throwable, ?], Task]("JDBC Composite")
    with JDBCProperties[ZStream[Any, Throwable, ?], Task] with ZIOProperties {

  import mapper.mapped

  lazy val compTable = mapped[Composite2].table("composite2").keys(Cols('pkLong, 'pkUUID))
  lazy val compRel3  = mapped[Composite3].table("composite3").keys(Cols('pkInt, 'pkString, 'pkBool))

  import sqlQueries._
  lazy val queries2 = {
    setup(compTable)
    Queries2(writes(compTable),
             byPK(compTable),
             rawSQLStream(S.emit(dialect.truncateTable(compTable.definition))))
  }

  lazy val queries3 = {
    setup(compRel3)
    Queries3(writes(compRel3),
             byPK(compRel3),
             rawSQLStream(S.emit(dialect.truncateTable(compRel3.definition))))
  }
}
