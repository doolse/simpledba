package test

import com.amazonaws.ClientConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import io.doolse.simpledba.dynamodb.DynamoDBMapper
import shapeless._
import cats.syntax.all._

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

  val qb = (QueryBuilder.queryByFullKey(mappedTable) |@|
    QueryBuilder.queryByFullKey(anotherTable) |@| QueryBuilder.queryByFullKey(mappedTable)).tupled
  val ((q1, q2, q3), _) = qb.build

}
