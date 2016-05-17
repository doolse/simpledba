package test

import cats.Monad
import cats.data.OptionT
import io.doolse.simpledba.dynamodb.{DynamoDBMapper, DynamoDBSession}
import shapeless._
import shapeless.syntax.singleton._
import cats.std.option._
import cats.syntax.all._
import com.amazonaws.ClientConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient

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

  implicit val embeddedColumnMapper = mapper.GenericColumnMapper[EmbeddedFields]

  case class Inst(uniqueid: Long, embedded: EmbeddedFields)

  case class User(firstName: String, lastName: String)

  //case class Inst(uniqueid: Long, adminpassword: String, enabled: Boolean)

  val instRecord = ('uniqueid ->> 0L) :: ('adminpassword ->> "hi") :: ('enabled ->> true) :: HNil
  val mappedTable = mapper.relation[Inst]("institution").key('uniqueid)
  val anotherTable = mapper.relation[User]("users").keys('firstName, 'lastName)

  val (creation, (qpk, writer, users, writeUsers)) = mapper.buildSchema(for {
    qpk <- mappedTable.queryByKey
    yo <- anotherTable.queryAllByKeyColumn('lastName)
    writer <- mappedTable.writeQueries
    writeUsers <- anotherTable.writeQueries
  } yield (qpk.as[Long], writer, yo.as[String], writeUsers)
  )
  creation.foreach(ct => Try {
    client.deleteTable(ct.getTableName)
    client.createTable(ct)
  } match {
    case Failure(f) => f.printStackTrace()
    case _ =>
  })
  val orig = Inst(1L, EmbeddedFields("pass", enabled = true))
  val updated = Inst(2L, EmbeddedFields("pass", enabled = false))
  val updatedAgain = Inst(2L, EmbeddedFields("changed", enabled = true))
  val q = for {
    _ <- writeUsers.insert(User("Jolse", "Maginnis"))
    _ <- writeUsers.insert(User("Emma", "Maginnis"))
    _ <- writer.insert(orig)
    res2 <- qpk.query(1L)
    res <- qpk.query(517573426L)
    upd1 <- writer.update(orig, updated)
    res3 <- qpk.query(2L)
    upd2 <- writer.update(updated, updatedAgain)
    res4 <- qpk.query(2L)
    all <- users.query("Maginnis")
    _ <-  res4.map(writer.delete).sequence
  } yield (all)
  val res = q.run(DynamoDBSession(client))
  println(res)

}
