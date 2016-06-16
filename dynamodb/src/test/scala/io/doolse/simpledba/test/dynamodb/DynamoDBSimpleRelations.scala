package io.doolse.simpledba.test.dynamodb

import cats.Id
import fs2.interop.cats._
import com.amazonaws.ClientConfiguration
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, AmazonDynamoDBAsyncClient}
import io.doolse.simpledba.BuiltQueries
import io.doolse.simpledba.dynamodb.{DynamoDBMapper, DynamoDBSession, DynamoDBUtils}
import io.doolse.simpledba.dynamodb.DynamoDBMapper.Effect
import io.doolse.simpledba.test.SimpleRelations._
import io.doolse.simpledba.test.SimpleRelations
/**
  * Created by jolz on 15/06/16.
  */
object DynamoDBSimpleRelations extends SimpleRelations[Effect]("DynamoDB") with DynamoDBProperties {

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
