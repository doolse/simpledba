package test

import com.amazonaws.ClientConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import io.doolse.simpledba.RelationModel
import io.doolse.simpledba.dynamodb.DynamoDBMapper
import io.doolse.simpledba.dynamodb.DynamoDBMapper.DynamoDBSession
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
  import mapper._

  val built = mapper.buildModel(TestCreator.model)

  val queries = built.queries
  val creation = built.ddl.value

  val existingTables = client.listTables().getTableNames.asScala.toSet
  creation.foreach(ct => Try {
    val tableName = ct.getTableName
    if (existingTables.contains(tableName)) {
      println("Deleting: " + tableName)
      client.deleteTable(tableName)
    }
    println("Creating: " + tableName)
    client.createTable(ct)
  } match {
    case Failure(f) => f.printStackTrace()
    case _ =>
  })
  val q = TestCreator.doTest(queries)
  val res = q.run(DynamoDBSession(client))
  println(res)

}
