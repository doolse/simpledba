package io.doolse.simpledba.test.jdbc

import io.doolse.simpledba.jdbc.Effect
import io.doolse.simpledba.test.{RangeQueryProperties, SimpleDBAProperties}
import org.scalacheck.Shapeless._

/**
  * Created by jolz on 3/07/16.
  */
object JDBCRangeQueries extends SimpleDBAProperties("JDBC") {

  include(new RangeQueryProperties[Effect] with JDBCProperties {
    lazy val queries: Queries[Effect] = setup(mapper.buildModel(model))
  })
}

