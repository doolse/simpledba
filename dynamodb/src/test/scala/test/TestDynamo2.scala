package test

import io.doolse.simpledba.dynamodb.{DynamoDBMapper2, DynamoDBSession}
import shapeless._
import shapeless.syntax.singleton._
import cats.std.option._
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient

/**
  * Created by jolz on 10/05/16.
  */
object TestDynamo2 extends App {

  val client: AmazonDynamoDBClient = new AmazonDynamoDBClient().withEndpoint("http://localhost:8000")
  val mapper = new DynamoDBMapper2()

  import mapper._

  case class EmbeddedFields(adminpassword: String, enabled: Boolean)

  implicit val embeddedColumnMapper = mapper.GenericColumnMapper[EmbeddedFields]

  case class Inst(uniqueid: Long, embedded: EmbeddedFields)

  //case class Inst(uniqueid: Long, adminpassword: String, enabled: Boolean)

  val instRecord = ('uniqueid ->> 0L) :: ('adminpassword ->> "hi") :: ('enabled ->> true) :: HNil
  val mappedTable = mapper.relation[Inst]("institution").key('uniqueid)
  val (qpk, writer) = mapper.build(for {
    qpk <- mappedTable.queryByKey
    writer <- mappedTable.writeQueries
  } yield (qpk.as[Long], writer)
  )
  val q = for {
    _ <- writer.insert(Inst(1L, EmbeddedFields("pass", true)))
    res <- qpk.query(517573426L)
    res2 <- qpk.query(1L)
  } yield (res, res2)
  val res = q.run(DynamoDBSession(client))
  println(res)

}
