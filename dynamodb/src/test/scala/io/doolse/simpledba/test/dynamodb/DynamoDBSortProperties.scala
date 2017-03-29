package io.doolse.simpledba.test.dynamodb

import fs2.interop.cats._
import io.doolse.simpledba.CatsUtils._
import io.doolse.simpledba.dynamodb.DynamoDBIO._
import io.doolse.simpledba.dynamodb.Effect
import io.doolse.simpledba.test.{SimpleDBAProperties, Sortable, SortedQueryProperties}
import org.scalacheck.Arbitrary
import org.scalacheck.Test.Parameters
import org.scalacheck.derive.MkArbitrary

/**
  * Created by jolz on 30/06/16.
  */
object DynamoDBSortProperties extends SimpleDBAProperties("DynamoDB") with DynamoDBProperties {
  override def overrideParameters(p: Parameters) = p.withMinSuccessfulTests(5)

  implicit val arbSortable = Arbitrary(MkArbitrary[Sortable].arbitrary.arbitrary.map(s => s.copy(doubleField = s.floatField)))

  include(new SortedQueryProperties[Effect] with DynamoDBProperties {
    val queries: Queries[Effect] = setup(mapper.buildModel(model))
  })
}
