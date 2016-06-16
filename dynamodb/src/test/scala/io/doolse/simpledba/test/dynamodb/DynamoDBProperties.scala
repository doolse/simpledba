package io.doolse.simpledba.test.dynamodb

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import io.doolse.simpledba.BuiltQueries
import io.doolse.simpledba.dynamodb.DynamoDBMapper._
import io.doolse.simpledba.dynamodb.{DynamoDBMapper, DynamoDBSession, DynamoDBUtils}

/**
  * Created by jolz on 16/06/16.
  */
trait DynamoDBProperties {
  lazy val client = DynamoDBUtils.createClient()

  lazy val mapper = new DynamoDBMapper()

  def setup[Q](bq: BuiltQueries.Aux[Q, CreateTableRequest]) = {
    DynamoDBUtils.createSchema(client, bq.ddl)
    bq.queries
  }

  def run[A](fa: Effect[A]): A = fa.run(DynamoDBSession(client)).unsafeRun
}
