package test

import com.amazonaws.ClientConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import io.doolse.simpledba.RelationModel
import io.doolse.simpledba.dynamodb.{DynamoDBMapper, DynamoDBSession, DynamoDBUtils}
import shapeless._
import shapeless.ops.hlist.RightFolder
import shapeless.syntax.singleton._

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}
/**
  * Created by jolz on 10/05/16.
  */
object TestDynamo extends App {

  val config = new ClientConfiguration().withProxyHost("localhost").withProxyPort(8888)
  val client: AmazonDynamoDBClient = new AmazonDynamoDBClient(config).withEndpoint("http://localhost:8000")

  val mapper = new DynamoDBMapper()
  val built = mapper.buildModel(TestCreator.model)

  val queries = built.queries
  val creation = built.ddl

  DynamoDBUtils.createSchema(client, built.ddl)
  val q = TestCreator.doTest(queries)
  val res = q.run(DynamoDBSession(client))
  println(res)

}
