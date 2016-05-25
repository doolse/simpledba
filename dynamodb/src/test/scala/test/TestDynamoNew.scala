package test

import com.amazonaws.ClientConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import io.doolse.simpledba.dynamodb.DynamoDBMapper
import shapeless._
import cats.syntax.all._
import cats.std.option._
import cats.sequence._
import io.doolse.simpledba.{MultiQuery, SingleQuery, WriteQueries}
import io.doolse.simpledba.dynamodb.DynamoDBMapper.DynamoDBSession

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

/**
  * Created by jolz on 23/05/16.
  */
object TestDynamoNew extends App {
  val config = new ClientConfiguration().withProxyHost("localhost").withProxyPort(8888)
  val client : AmazonDynamoDBClient = new AmazonDynamoDBClient(config).withEndpoint("http://localhost:8000")
  val mapper = new DynamoDBMapper()
import mapper._

  case class EmbeddedFields(adminpassword: String, enabled: Boolean)

  implicit val embeddedColumnMapper = mapper.GenericColumnMapper[EmbeddedFields]

  case class Inst(uniqueid: Long, embedded: EmbeddedFields)

  case class Username(fn: String, ln: String)
  case class User(firstName: String, lastName: String, year: Int)

  case class Queries[F[_]](instByPK: SingleQuery[F, Inst, Long],
                           writeInst: WriteQueries[F, Inst],
                           querybyFirstName: MultiQuery[F, User, String],
                           writeUsers: WriteQueries[F, User],
                           queryByLastName: MultiQuery[F, User, String],
                           queryByFullName: SingleQuery[F, User, Username]
                          )



  //case class Inst(uniqueid: Long, adminpassword: String, enabled: Boolean)

  val instTable = mapper.relation[Inst]("institution").key('uniqueid)
  val userTable = mapper.relation[User]("users").keys('firstName, 'lastName)

  val allBuilders = HList(
    queryByFullKey(instTable),
    writeQueries(instTable),
    queryAllByKeyColumn('firstName, userTable),
    writeQueries(userTable),
    queryAllByKeyColumn('lastName, userTable),
      queryByFullKey(userTable)
  )

  val built = build(allBuilders)
  val queries = built.as[Queries]()
  val creation = built.ddl.value
  import queries._


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
  val orig = Inst(1L, EmbeddedFields("pass", enabled = true))
  val updated = Inst(2L, EmbeddedFields("pass", enabled = false))
  val updatedAgain = Inst(2L, EmbeddedFields("changed", enabled = true))
  val q = for {
    _ <- writeUsers.insert(User("Jolse", "Maginnis", 1980))
    _ <- writeUsers.insert(User("Emma", "Maginnis", 1982))
    _ <- writeUsers.insert(User("Jolse", "Fuckstick", 1980))
    _ <- writeInst.insert(orig)
    res2 <- instByPK.query(1L)
    res <- instByPK.query(517573426L)
    upd1 <- writeInst.update(orig, updated)
    res3 <- instByPK.query(2L)
    upd2 <- writeInst.update(updated, updatedAgain)
    res4 <- instByPK.query(2L)
    all <- queryByLastName.queryWithOrder("Maginnis", true)
    allFirst <- queryByFullName.query(Username("Jolse", "Maginnis"))
    _ <-  res4.map(writeInst.delete).sequence
  } yield (res2, res, upd1, res3, upd2, res4, all, allFirst)
  val res = q.run(DynamoDBSession(client))
  println(res)
}
