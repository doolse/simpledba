package io.doolse.simpledba.test.jdbc

import io.doolse.simpledba.jdbc.stdImplicits._
import io.doolse.simpledba.jdbc.Effect
import io.doolse.simpledba.test.{SimpleDBAProperties, SortedQueryProperties}
import org.scalacheck.Shapeless._

/**
  * Created by jolz on 21/06/16.
  */
object JDBCSortedQueries extends SimpleDBAProperties("JDBC") {
  include(new SortedQueryProperties[Effect] with JDBCProperties {
    lazy val queries: Queries[Effect] = setup(mapper.buildModel(model))
  })
}
