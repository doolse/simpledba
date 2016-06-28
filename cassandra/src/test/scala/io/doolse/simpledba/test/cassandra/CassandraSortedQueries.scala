package io.doolse.simpledba.test.cassandra

import io.doolse.simpledba.cassandra.CassandraMapper.Effect
import io.doolse.simpledba.test.SortedQueryProperties
import fs2.interop.cats._

/**
  * Created by jolz on 21/06/16.
  */
object CassandraSortedQueries extends SortedQueryProperties[Effect]("Cassandra") with CassandraProperties {
  lazy val queries: Queries[Effect] = setup(mapper.buildModel(model))
}
