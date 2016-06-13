package io.doolse.simpledba.dynamodb

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest

import scala.util.{Failure, Try}
import scala.collection.JavaConverters._

/**
  * Created by jolz on 13/06/16.
  */
object DynamoDBUtils {
  def createSchema(client: AmazonDynamoDBClient, creation: List[CreateTableRequest]): Unit = {
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
  }

}
