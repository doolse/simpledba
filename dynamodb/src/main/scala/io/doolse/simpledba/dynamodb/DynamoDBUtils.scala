package io.doolse.simpledba.dynamodb

import java.util.concurrent.TimeUnit

import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.{ClientConfiguration, PredefinedClientConfigurations}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBAsync, AmazonDynamoDBAsyncClient, AmazonDynamoDBClient}
import com.amazonaws.services.dynamodbv2.model.{CreateTableRequest, DeleteTableRequest, DescribeTableRequest, DescribeTableResult, ListTablesRequest}
import com.typesafe.config.{Config, ConfigFactory}
import fs2.Task
import cats.instances.vector._
import cats.syntax.all._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.internal.StaticCredentialsProvider
import io.doolse.simpledba.CatsUtils._

import scala.util.{Failure, Try}
import scala.collection.JavaConverters._
import fs2.interop.cats._
import fs2._
import fs2.util.Async
import fs2.Task._
import DynamoDBIO._

import scala.concurrent.duration.FiniteDuration

/**
  * Created by jolz on 13/06/16.
  */
object DynamoDBUtils {

  def createSchema(s: DynamoDBSession, deleteTables: Boolean, creation: Iterable[CreateTableRequest]): Task[Unit] = for {
    tr <- s.request(listTablesAsync, new ListTablesRequest())
    existingTables = tr.getTableNames.asScala.toSet
    allTables = creation.toVector
    tableNames = allTables.map(_.getTableName).toSet
    tablesToDelete = if (deleteTables) (tableNames & existingTables).toVector else Vector.empty
    missingTables = if (deleteTables) tableNames else tableNames &~ existingTables
    _ <- Stream(tablesToDelete: _*).evalMap(t => s.request(deleteTableAsync, new DeleteTableRequest(t))).run
    _ <- waitForStatus(s, tablesToDelete, None).run
    _ <- Stream(allTables: _*).filter(cr => missingTables(cr.getTableName)).evalMap(ct => s.request(createTableAsync, ct)).run
    _ <- waitForStatus(s, tableNames.toVector, Some("ACTIVE")).run
  } yield ()

  def waitForStatus(s: DynamoDBSession, tableNames: Vector[String], status: Option[String]): Stream[Task, Unit] = for {
    statii <- Stream.eval(statuses(s, tableNames).runLog)
    notReady = statii.filter(_._2 != status)
    _ <- if (notReady.isEmpty) Stream.empty else fs2.time.sleep(FiniteDuration(2, TimeUnit.SECONDS)) ++ waitForStatus(s, notReady.map(_._1).toVector, status)
  } yield ()

  def statuses(s: DynamoDBSession, tableNames: Vector[String]) = for {
    tn <- Stream(tableNames: _*)
    dtrA <- Stream.eval(s.request(describeTableAsync, new DescribeTableRequest(tn)).attempt)
  } yield {
    val ts = dtrA match {
      case Left(_) => None
      case Right(dtr) => Some(dtr.getTable.getTableStatus)
    }
    (tn, ts)
  }

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
