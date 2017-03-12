package io.doolse.simpledba.test.jdbc

import fs2.interop.cats._
import io.doolse.simpledba.CatsUtils._
import io.doolse.simpledba.jdbc.JDBCMapper.Effect
import io.doolse.simpledba.test.SimpleRelations
import io.doolse.simpledba.test.SimpleRelations._

/**
  * Created by jolz on 15/06/16.
  */
object JDBCSimpleRelations extends SimpleRelations[Effect]("JDBC") with JDBCProperties {

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
