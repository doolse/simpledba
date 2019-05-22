package io.doolse.simpledba.test

import cats.effect.IO
import fs2.Stream
import org.scalacheck.Shapeless._
import io.doolse.simpledba.jdbc.{BinOp, JDBCIO}
import shapeless._
import syntax.singleton._
import record._

object JDBCSortedProperties extends SimpleDBAProperties("JDBC") {
  include(new SortedQueryProperties[fs2.Stream, IO] with JDBCProperties {

    override val queries = {

      val sortedTable = mapper.mapped[Sortable].table("sorted").key('pk1)

      setup(sortedTable)
      import sqlQueries._

      val sameQuery  = query(sortedTable).where('same, BinOp.EQ)
      val sortWrites = writes(sortedTable)
      val truncate   = rawSQLStream(Stream.emit(dialect.truncateTable(sortedTable.definition)))

      def makeQueries(asc: Boolean) = {
        Queries(
          sortWrites,
          truncate,
          sameQuery.orderBy('intField, asc).build,
          sameQuery.orderWith(('intField ->> asc) :: ('stringField ->> asc) :: HNil).build,
          sameQuery.orderBy('stringField, asc).build,
          sameQuery.orderWith(('stringField ->> asc) :: ('shortField ->> asc) :: HNil).build,
          sameQuery.orderBy('shortField, asc).build,
          sameQuery.orderWith(('shortField ->> asc) :: ('doubleField ->> asc) :: HNil).build,
          sameQuery.orderBy('longField, asc).build,
          sameQuery.orderWith(('longField ->> asc) :: ('floatField ->> asc) :: HNil).build,
          sameQuery.orderBy('floatField, asc).build,
          sameQuery.orderWith(('floatField ->> asc) :: ('uuidField ->> asc) :: HNil).build,
          sameQuery.orderBy('doubleField, asc).build,
          sameQuery.orderWith(('doubleField ->> asc) :: ('intField ->> asc) :: HNil).build,
          sameQuery.orderBy('uuidField, asc).build,
          sameQuery.orderWith(('uuidField ->> asc) :: ('longField ->> asc) :: HNil).build
        )
      }
      (makeQueries(true), makeQueries(false))
    }
  })
}
