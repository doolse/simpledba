package io.doolse.simpledba.dynamodb

import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials}
import com.amazonaws.{ClientConfiguration, PredefinedClientConfigurations}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBAsync, AmazonDynamoDBAsyncClient, AmazonDynamoDBClient}
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.{Failure, Try}
import scala.collection.JavaConverters._

/**
  * Created by jolz on 13/06/16.
  */
object DynamoDBUtils {
  def createSchema(client: AmazonDynamoDB, creation: List[CreateTableRequest]): Unit = {
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

  def createClient(_config: Config = ConfigFactory.load()) : AmazonDynamoDBAsync = {
    val config = _config.getConfig("simpledba.dynamodb")
    val clientConfig = PredefinedClientConfigurations.dynamoDefault()
    if (config.hasPath("proxy")) {
      clientConfig.setProxyHost(config.getString("proxy.host"))
      clientConfig.setProxyPort(config.getInt("proxy.port"))
    }
    val client = if (config.hasPath("aws")) {
      val cConfig = config.getConfig("aws")
      val creds = new BasicAWSCredentials(cConfig.getString("accessKeyId"), cConfig.getString("secretKey"))
      val executor = java.util.concurrent.Executors.newFixedThreadPool(clientConfig.getMaxConnections)
      new AmazonDynamoDBAsyncClient(creds, clientConfig, executor)
    } else new AmazonDynamoDBAsyncClient(clientConfig)
    if (config.hasPath("endpoint")) client.withEndpoint(config.getString("endpoint")) else client
  }
}
