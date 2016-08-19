package io.doolse.simpledba.dynamodb

import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials}
import com.amazonaws.{ClientConfiguration, PredefinedClientConfigurations}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBAsync, AmazonDynamoDBAsyncClient, AmazonDynamoDBClient}
import com.amazonaws.services.dynamodbv2.model.{CreateTableRequest, DeleteTableRequest, ListTablesRequest}
import com.typesafe.config.{Config, ConfigFactory}
import fs2.Task
import DynamoDBIO._
import cats.std.vector._
import cats.syntax.all._
import io.doolse.simpledba.CatsUtils._

import scala.util.{Failure, Try}
import scala.collection.JavaConverters._
import fs2.interop.cats._

/**
  * Created by jolz on 13/06/16.
  */
object DynamoDBUtils {
  def createSchema(s: DynamoDBSession, deleteTables: Boolean, creation: Iterable[CreateTableRequest]): Task[Unit] = for {
    tr <- s.request(listTablesAsync, new ListTablesRequest())
    existingTables = tr.getTableNames.asScala.toSet
    _ <- creation.toVector.traverseU { ct =>
      val tableName = ct.getTableName
      val exists = existingTables(tableName)
      whenM(exists && deleteTables, s.request(deleteTableAsync, new DeleteTableRequest(tableName))) *>
      whenM(!exists || deleteTables, s.request(createTableAsync, ct))
    }
  } yield ()

  def createClient(_config: Config = ConfigFactory.load()): AmazonDynamoDBAsync = {
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
