package io.doolse.simpledba.test.cassandra

import fs2.interop.cats._
import io.doolse.simpledba.CatsUtils._
import io.doolse.simpledba.cassandra.CassandraIO._
import io.doolse.simpledba.cassandra.CassandraMapper.Effect
import io.doolse.simpledba.test.SimpleRelations
import io.doolse.simpledba.test.SimpleRelations._

/**
  * Created by jolz on 15/06/16.
  */
object CassandraSimpleRelations extends SimpleRelations[Effect]("Cassandra") with CassandraProperties {


  lazy val queries1: Fields1Queries[Effect] = {
    setup(mapper.buildModel(fields1Model))
  }

  lazy val queries2: Fields2Queries[Effect] = {
    setup(mapper.buildModel(fields2Model))
  }

  lazy val queries3: Fields3Queries[Effect] = {
    setup(mapper.buildModel(fields3Model))
  }
}
