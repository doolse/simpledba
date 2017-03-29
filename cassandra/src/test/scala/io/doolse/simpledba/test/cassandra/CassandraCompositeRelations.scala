package io.doolse.simpledba.test.cassandra

import io.doolse.simpledba.cassandra.stdImplicits._
import io.doolse.simpledba.cassandra.Effect
import io.doolse.simpledba.test.CompositeRelations
import io.doolse.simpledba.test.CompositeRelations._

/**
  * Created by jolz on 15/06/16.
  */
object CassandraCompositeRelations extends CompositeRelations[Effect]("Cassandra") with CassandraProperties {


  lazy val queries2: Queries2[Effect] = {
    setup(mapper.buildModel(composite2Model))
  }

  lazy val queries3: Queries3[Effect] = {
    setup(mapper.buildModel(composite3Model))
  }
}
