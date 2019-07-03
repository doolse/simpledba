package io.doolse.simpledba.test.jdbc

import cats.effect.IO
import fs2.Stream
import org.scalacheck.Shapeless._
import io.doolse.simpledba.jdbc.{BinOp, JDBCWriteOp}
import io.doolse.simpledba.test.{SimpleDBAProperties, Sortable, SortedQueryProperties}
import shapeless._
import syntax.singleton._

object JDBCSortedProperties extends SimpleDBAProperties("JDBC") {
  include(new SortedQueryProperties[fs2.Stream[IO, ?], IO, JDBCWriteOp] with JDBCProperties[fs2.Stream[IO, ?], IO] with FS2Properties {

    override val queries = {

      val sortedTable = mapper.mapped[Sortable].table("sorted").key('pk1)

      setup(sortedTable)
      import sqlQueries._

      val sameQuery  = query(sortedTable).where('same, BinOp.EQ)
      val sortWrites = writes(sortedTable)
      val truncate   = rawSQLStream(Stream.emit(dialect.truncateTable(sortedTable.definition)))

      def makeQueries(asc: Boolean) = {
        Queries(
          sameQuery.orderBy('intField, asc).build,
          sameQuery.orderWith(('intField ->> asc) :: ('stringField ->> asc) :: HNil).build,
          sameQuery.orderBy('stringField, asc).build,
          sameQuery.orderWith(('stringField ->> asc) :: ('shortField ->> asc) :: HNil).build,
          sameQuery.orderBy('shortField, asc).build,
          sameQuery.orderWith(('shortField ->> asc) :: ('uuidField ->> asc) :: HNil).build,
          sameQuery.orderBy('longField, asc).build,
          sameQuery.orderWith(('longField ->> asc) :: ('floatField ->> asc) :: HNil).build,
          sameQuery.orderBy('floatField, asc).build,
          sameQuery.orderWith(('floatField ->> asc) :: ('uuidField ->> asc) :: HNil).build,
          sameQuery.orderBy('uuidField, asc).build,
          sameQuery.orderWith(('uuidField ->> asc) :: ('longField ->> asc) :: HNil).build,
          sortWrites,
          truncate
        )
      }
      (makeQueries(true), makeQueries(false))
    }
  })
}
