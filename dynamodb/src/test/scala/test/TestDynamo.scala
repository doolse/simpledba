package test

import cats.Monad
import cats.data.OptionT
import shapeless._
import shapeless.syntax.singleton._
import cats.std.option._
import cats.syntax.all._
import com.amazonaws.ClientConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import io.doolse.simpledba.dynamodb.DynamoDBMapper
import io.doolse.simpledba.dynamodb.DynamoDBMapper.DynamoDBSession
import scala.collection.JavaConverters._

import scala.util.{Failure, Try}

/**
  * Created by jolz on 10/05/16.
  */
object TestDynamo extends App {

  val config = new ClientConfiguration().withProxyHost("localhost").withProxyPort(8888)
  val client : AmazonDynamoDBClient = new AmazonDynamoDBClient(config).withEndpoint("http://localhost:8000")
  val mapper = new DynamoDBMapper()

  import mapper._

  case class EmbeddedFields(adminpassword: String, enabled: Boolean)

  val newContext = mapper.emptyContext.embed[EmbeddedFields]

  case class Inst(uniqueid: Long, embedded: EmbeddedFields)

  case class Username(fn: String, ln: String)
  case class User(firstName: String, lastName: String, year: Int)

  //case class Inst(uniqueid: Long, adminpassword: String, enabled: Boolean)

  val instRecord = ('uniqueid ->> 0L) :: ('adminpassword ->> "hi") :: ('enabled ->> true) :: HNil
  val mappedTable = mapper.relation("institution", newContext.lookup[Inst]).key('uniqueid)
  val anotherTable = mapper.relation("users", newContext.lookup[User]).keys('firstName, 'lastName)

//  val (creation, (qpk, writer, users, byFirstName, writeUsers)) = mapper.buildSchema(for {
//    qpk <- mappedTable.queryByKey
//    byLastName <- anotherTable.queryAllByKeyColumn('lastName)
//    byFirstName <- anotherTable.queryAllByKeyColumn('firstName)
//    byFullName <- anotherTable.queryByKey
//    writer <- mappedTable.writeQueries
//    writeUsers <- anotherTable.writeQueries
//  } yield (qpk.as[Long], writer, byLastName.as[String], byFullName.as[Username], writeUsers)
//  )
//
//  val existingTables = client.listTables().getTableNames.asScala.toSet
//  creation.foreach(ct => Try {
//    val tableName = ct.getTableName
//    if (existingTables.contains(tableName)) {
//      println("Deleting: " + tableName)
//      client.deleteTable(tableName)
//    }
//    println("Creating: " + tableName)
//    client.createTable(ct)
//  } match {
//    case Failure(f) => f.printStackTrace()
//    case _ =>
//  })
//  val orig = Inst(1L, EmbeddedFields("pass", enabled = true))
//  val updated = Inst(2L, EmbeddedFields("pass", enabled = false))
//  val updatedAgain = Inst(2L, EmbeddedFields("changed", enabled = true))
//  val q = for {
//    _ <- writeUsers.insert(User("Jolse", "Maginnis", 1980))
//    _ <- writeUsers.insert(User("Emma", "Maginnis", 1982))
//    _ <- writeUsers.insert(User("Jolse", "Fuckstick", 1980))
//    _ <- writer.insert(orig)
//    res2 <- qpk.query(1L)
//    res <- qpk.query(517573426L)
//    upd1 <- writer.update(orig, updated)
//    res3 <- qpk.query(2L)
//    upd2 <- writer.update(updated, updatedAgain)
//    res4 <- qpk.query(2L)
//    all <- users.queryWithOrder("Maginnis", true)
//    allFirst <- byFirstName.query(Username("Jolse", "Maginnis"))
//    _ <-  res4.map(writer.delete).sequence
//  } yield (res2, res, upd1, res3, upd2, res4, all, allFirst)
//  val res = q.run(DynamoDBSession(client))
//  println(res)

}
