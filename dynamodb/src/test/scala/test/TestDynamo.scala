package test

import com.amazonaws.ClientConfiguration
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, AmazonDynamoDBAsyncClient}
import fs2.interop.cats._
import io.doolse.simpledba.dynamodb.{DynamoDBMapper, DynamoDBSession, DynamoDBUtils}
import io.doolse.simpledba.test.TestCreator
/**
  * Created by jolz on 10/05/16.
  */
object TestDynamo extends App {

  val config = new ClientConfiguration().withProxyHost("localhost").withProxyPort(8888)
  val client: AmazonDynamoDBAsync = new AmazonDynamoDBAsyncClient(config).withEndpoint("http://localhost:8000")

  val mapper = new DynamoDBMapper()
  val built = mapper.buildModel(TestCreator.model)

  val queries = built.queries
  val creation = built.ddl

  DynamoDBUtils.createSchema(client, built.ddl)
  val q = TestCreator.doTest(queries)
  val res = q.run(DynamoDBSession(client)).unsafeRun
  println(res)

}
