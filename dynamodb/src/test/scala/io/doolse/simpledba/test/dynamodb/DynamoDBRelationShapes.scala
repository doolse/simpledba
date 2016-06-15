package io.doolse.simpledba.test.dynamodb

import cats.Id
import fs2.interop.cats._
import com.amazonaws.ClientConfiguration
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, AmazonDynamoDBAsyncClient}
import io.doolse.simpledba.BuiltQueries
import io.doolse.simpledba.dynamodb.{DynamoDBMapper, DynamoDBSession, DynamoDBUtils}
import io.doolse.simpledba.dynamodb.DynamoDBMapper.Effect
import io.doolse.simpledba.test.RelationShapes._
import io.doolse.simpledba.test.RelationShapes
/**
  * Created by jolz on 15/06/16.
  */
object DynamoDBRelationShapes extends RelationShapes[Effect]("DynamoDB") {
  lazy val config = new ClientConfiguration()
  lazy val client: AmazonDynamoDBAsync = new AmazonDynamoDBAsyncClient(config).withEndpoint("http://localhost:8000")

  lazy val mapper = new DynamoDBMapper()

  def setup[Q](bq: BuiltQueries.Aux[Q, CreateTableRequest]) = {
    DynamoDBUtils.createSchema(client, bq.ddl)
    bq.queries
  }

  def run[A](fa: Effect[A]): A = fa.run(DynamoDBSession(client)).unsafeRun

  lazy val gen1: Fields1Queries[Effect] = {
    setup(mapper.buildModel(fields1Model))
  }

  lazy val gen2: Fields2Queries[Effect] = {
    setup(mapper.buildModel(fields2Model))
  }

  lazy val gen3: Fields3Queries[Effect] = {
    setup(mapper.buildModel(fields3Model))
  }
}
