package io.doolse.simpledba.dynamodb.test

import java.net.URI

import io.doolse.simpledba.dynamodb.DynamoDBMapper
import shapeless.syntax.singleton._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{CreateTableRequest, ProvisionedThroughput}

case class MyTest(name: String, frogs: Int)

object DynamoDBTest extends App {
  val mapper = new DynamoDBMapper

  val simpleTable = mapper.mapped[MyTest].table("hello", 'name)

  private val definiton: CreateTableRequest = simpleTable.tableDefiniton
    .provisionedThroughput(
      ProvisionedThroughput.builder()
        .readCapacityUnits(1L)
        .writeCapacityUnits(1L).build())
    .build()
  println(definiton)

  val builder = DynamoDbClient.builder()

  val localClient =
    builder.region(Region.US_EAST_1).endpointOverride(URI.create("http://localhost:8000")).build()
  localClient.createTable(definiton)
}
