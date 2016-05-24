package test

import com.amazonaws.ClientConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import io.doolse.simpledba.dynamodb.DynamoDBMapper
import shapeless._
import cats.syntax.all._
import cats.sequence._
import io.doolse.simpledba.WriteQueries
import io.doolse.simpledba.dynamodb.DynamoDBMapper.DynamoDBSession

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

  //case class Inst(uniqueid: Long, adminpassword: String, enabled: Boolean)

  val mappedTable = mapper.relation[Inst]("institution").key('uniqueid)
  val anotherTable = mapper.relation[User]("users").keys('firstName, 'lastName)

  val allBuilders = HList(QueryBuilder.queryByFullKey(mappedTable),
    QueryBuilder.queryByFullKey(anotherTable), QueryBuilder.writeQueries(mappedTable))

  val (q1, q2, wq : WriteQueries[DynamoDBMapper.Effect, Inst]) = QueryBuilder.build(allBuilders).tupled

  val res : DynamoDBMapper.Effect[_] = for {
    _ <- wq.insert(Inst(2L, EmbeddedFields("asd", true)))
    q <- q1.as[Long].query(2L)
  } yield q

  println(res.run(DynamoDBSession(client)))
}
