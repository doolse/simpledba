package test

import com.amazonaws.ClientConfiguration
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, AmazonDynamoDBAsyncClient}
import io.doolse.simpledba.dynamodb.{DynamoDBMapper, DynamoDBSession, DynamoDBUtils}
import io.doolse.simpledba.test.TestCreator
import io.doolse.simpledba.dynamodb.DynamoDBIO._
/**
  * Created by jolz on 10/05/16.
  */
object TestDynamo extends App {

  val config = new ClientConfiguration().withProxyHost("localhost").withProxyPort(8888)
  val client: AmazonDynamoDBAsync = new AmazonDynamoDBAsyncClient(config).withEndpoint("http://localhost:8000")

  val mapper = new DynamoDBMapper()
  val built = mapper.verifyModel(TestCreator.model)

  val queries = built.queries
  val creation = built.ddl

  val session = DynamoDBSession(client)
  DynamoDBUtils.createSchema(session, true, built.ddl).unsafeRunSync()
  val q = TestCreator.doTest(queries)
  val res = q.run(session).unsafeRunSync()
  println(res)

}
