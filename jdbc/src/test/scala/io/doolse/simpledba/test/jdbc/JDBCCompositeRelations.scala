package io.doolse.simpledba.test.jdbc

import io.doolse.simpledba.jdbc.Effect
import io.doolse.simpledba.test.CompositeRelations
import io.doolse.simpledba.test.CompositeRelations._
import io.doolse.simpledba.jdbc.stdImplicits._

/**
  * Created by jolz on 15/06/16.
  */
object JDBCCompositeRelations extends CompositeRelations[Effect]("JDBC") with JDBCProperties {


  lazy val queries2: Queries2[Effect] = {
    setup(mapper.buildModel(composite2Model))
  }

  lazy val queries3: Queries3[Effect] = {
    setup(mapper.buildModel(composite3Model))
  }
}
