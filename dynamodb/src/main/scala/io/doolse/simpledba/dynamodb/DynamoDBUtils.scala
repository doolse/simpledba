package io.doolse.simpledba.dynamodb

import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.{ClientConfiguration, PredefinedClientConfigurations}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBAsync, AmazonDynamoDBAsyncClient, AmazonDynamoDBClient}
import com.amazonaws.services.dynamodbv2.model.{CreateTableRequest, DeleteTableRequest, ListTablesRequest}
import com.typesafe.config.{Config, ConfigFactory}
import fs2.Task
import DynamoDBIO._
import cats.std.vector._
import cats.syntax.all._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.internal.StaticCredentialsProvider
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
    val creds = if (config.hasPath("credentials")) {
      val cConfig = config.getConfig("credentials")
      if (cConfig.hasPath("profile")) {
        new ProfileCredentialsProvider(cConfig.getString("profile"))
      } else new StaticCredentialsProvider(new BasicAWSCredentials(cConfig.getString("accessKeyId"), cConfig.getString("secretKey")))
    } else new DefaultAWSCredentialsProviderChain()
    val client = new AmazonDynamoDBAsyncClient(creds, clientConfig)
    if (config.hasPath("endpoint")) client.withEndpoint(config.getString("endpoint")) else client
  }
}
