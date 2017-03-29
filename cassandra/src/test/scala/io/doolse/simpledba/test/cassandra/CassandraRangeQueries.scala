package io.doolse.simpledba.test.cassandra

import io.doolse.simpledba.cassandra._
import io.doolse.simpledba.test.{RangeQueryProperties, SimpleDBAProperties}
import fs2.interop.cats._
import io.doolse.simpledba.CatsUtils._
import io.doolse.simpledba.cassandra.CassandraIO._
import org.scalacheck.Shapeless._

/**
  * Created by jolz on 3/07/16.
  */
object CassandraRangeQueries extends SimpleDBAProperties("Cassandra") {

  include(new RangeQueryProperties[Effect] with CassandraProperties {
    lazy val queries: Queries[Effect] = setup(mapper.buildModel(model))
  })
}

