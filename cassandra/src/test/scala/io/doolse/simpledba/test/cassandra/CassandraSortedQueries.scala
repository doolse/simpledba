package io.doolse.simpledba.test.cassandra

import io.doolse.simpledba.cassandra._
import io.doolse.simpledba.test.{SimpleDBAProperties, SortedQueryProperties}
import io.doolse.simpledba.CatsUtils._
import org.scalacheck.Shapeless._

/**
  * Created by jolz on 21/06/16.
  */
object CassandraSortedQueries extends SimpleDBAProperties("Cassandra") {
  include(new SortedQueryProperties[Effect] with CassandraProperties {
    lazy val queries: Queries[Effect] = setup(mapper.buildModel(model))
  })
}
