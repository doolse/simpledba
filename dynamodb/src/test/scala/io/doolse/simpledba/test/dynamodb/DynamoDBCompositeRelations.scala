package io.doolse.simpledba.test.dynamodb

import io.doolse.simpledba.dynamodb.Effect
import io.doolse.simpledba.dynamodb.stdImplicits._
import io.doolse.simpledba.test.CompositeRelations
import io.doolse.simpledba.test.CompositeRelations._

/**
  * Created by jolz on 15/06/16.
  */
object DynamoDBCompositeRelations extends CompositeRelations[Effect]("DynamoDB") with DynamoDBProperties {


  lazy val queries2: Queries2[Effect] = {
    setup(mapper.buildModel(composite2Model))
  }

  lazy val queries3: Queries3[Effect] = {
    setup(mapper.buildModel(composite3Model))
  }
}
