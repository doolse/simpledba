package test

import cats.Monad
import cats.data.OptionT
import io.doolse.simpledba.dynamodb.{DynamoDBMapper2, DynamoDBSession}
import shapeless._
import shapeless.syntax.singleton._
import cats.std.option._
import cats.syntax.all._
import com.amazonaws.ClientConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient

/**
  * Created by jolz on 10/05/16.
  */
object TestDynamo2 extends App {

  val config = new ClientConfiguration().withProxyHost("localhost").withProxyPort(8888)
  val client : AmazonDynamoDBClient = new AmazonDynamoDBClient(config).withEndpoint("http://localhost:8000")
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
  val orig = Inst(1L, EmbeddedFields("pass", true))
  val updated = Inst(2L, EmbeddedFields("pass", false))
  val updatedAgain = Inst(2L, EmbeddedFields("changed", true))
  val q = for {
    _ <- writer.insert(orig)
    res2 <- qpk.query(1L)
    res <- qpk.query(517573426L)
    upd1 <- writer.update(orig, updated)
    res3 <- qpk.query(2L)
    upd2 <- writer.update(updated, updatedAgain)
    res4 <- qpk.query(2L)
    _ <-  res4.map(writer.delete).sequence
  } yield (res, res2, res3, upd1, upd2)
  val res = q.run(DynamoDBSession(client))
  println(res)

}
