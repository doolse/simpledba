package io.doolse.simpledba.test.dynamodb

import io.doolse.simpledba.CatsUtils._
import io.doolse.simpledba.{Exclusive, FilterRange}
import io.doolse.simpledba.dynamodb.DynamoDBIO._
import io.doolse.simpledba.dynamodb.Effect
import io.doolse.simpledba.test._
import org.scalacheck.Arbitrary
import org.scalacheck.Test.Parameters
import org.scalacheck.derive.MkArbitrary
import org.scalacheck.Shapeless._

/**
  * Created by jolz on 30/06/16.
  */
object DynamoDBRangeProperties extends SimpleDBAProperties("DynamoDB") with DynamoDBProperties {

  override def overrideParameters(p: Parameters) = p.withMinSuccessfulTests(5)

  implicit val arbRangeable = Arbitrary(MkArbitrary[Rangeable].arbitrary.arbitrary.map(s => s.copy(doubleField = s.floatField)))

  include(new RangeQueryProperties[Effect] with DynamoDBProperties {
    val queries: Queries[Effect] = setup(mapper.buildModel(model))
  })
}
