package io.doolse.simpledba.test.dynamodb

import io.doolse.simpledba.dynamodb.DynamoDBMapper.Effect
import io.doolse.simpledba.test.SortedQueryProperties
import fs2.interop.cats._
import io.doolse.simpledba.dynamodb.DynamoDBIO._
import io.doolse.simpledba.CatsUtils._
import org.scalacheck.Arbitrary
import org.scalacheck.Shapeless._

/**
  * Created by jolz on 30/06/16.
  */
object DynamoDBSortProperties extends SortedQueryProperties[Effect]("DynamoDB") with DynamoDBProperties {
  val queries: Queries[Effect] = setup(mapper.buildModel(model))

  override def genSortable = for {
    v <- Arbitrary.arbitrary[Vector[Sortable]]
  } yield {
    v.map { s =>
      s.copy(doubleField = s.floatField.toDouble)
    }
  }
}
