package test

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model._
import io.doolse.simpledba.dynamodb.{DynamoDBColumn, DynamoDBMapper, DynamoDBRelationIO, DynamoDBSession}
import shapeless.labelled.FieldType

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Created by jolz on 5/05/16.
  */
object TestDynamo extends App {


  val client: AmazonDynamoDBClient = new AmazonDynamoDBClient().withEndpoint("http://localhost:8000")

  //  val res = client.createTable(List(
  //    new AttributeDefinition("uniqueid", ScalarAttributeType.N)
  //  ).asJava, "institution",
  //    List(new KeySchemaElement("uniqueid", KeyType.HASH)).asJava,
  //    new ProvisionedThroughput(1L, 1L))

  case class Inst(uniqueid: Long, adminpassword: String, enabled: Boolean)

  val mapper = new DynamoDBMapper()

  import mapper._

  val table = mapper.table[Inst]("institution").key('uniqueid)
  val tableDDL = mapper.generateDDL(table)

  Try { client.deleteTable("institution") }
  client.createTable(tableDDL)

  val res = client.putItem("institution", Map(
    "uniqueid" -> new AttributeValue().withN(517573426L.toString),
    "unknown" -> new AttributeValue().withNULL(true),
    "adminpassword" -> new AttributeValue("SHA256:59aa63c9bbdc51a05db11fa61c465f35a1b88c1c6a94bbd7d3872e18a217ae9c"),
    "enabled" -> new AttributeValue().withBOOL(true)
  ).asJava)


  val longCol = DynamoDBColumn[Long](_.getN.toLong, l => new AttributeValue().withN(l.toString), ScalarAttributeType.N)
  val boolCol = DynamoDBColumn[Boolean](_.getBOOL, l => new AttributeValue().withBOOL(l), ScalarAttributeType.S)
  val stringCol = DynamoDBColumn[String](_.getS, new AttributeValue(_), ScalarAttributeType.S)
  val dynamoDB = new DynamoDBRelationIO()

  println(TestQuery.doQuery(dynamoDB)(boolCol, stringCol, longCol).run(DynamoDBSession(client)))
}
