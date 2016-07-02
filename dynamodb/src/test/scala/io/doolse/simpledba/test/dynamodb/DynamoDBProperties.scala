package io.doolse.simpledba.test.dynamodb

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import org.scalacheck.Arbitrary.arbString
import io.doolse.simpledba.BuiltQueries
import io.doolse.simpledba.dynamodb.DynamoDBMapper._
import io.doolse.simpledba.dynamodb.{DynamoDBColumn, DynamoDBMapper, DynamoDBSession, DynamoDBUtils}
import org.scalacheck.Arbitrary

/**
  * Created by jolz on 16/06/16.
  */
trait DynamoDBProperties {

  implicit val arbString : Arbitrary[String] = Arbitrary {
    Arbitrary.arbString.arbitrary.map {
      case DynamoDBColumn.EmptyStringValue => ""
      case o => o
    }
  }

  lazy val client = DynamoDBUtils.createClient()

  lazy val session = DynamoDBSession(client)

  lazy val mapper = new DynamoDBMapper()

  def setup[Q](bq: BuiltQueries.Aux[Q, CreateTableRequest]) = {
    DynamoDBUtils.createSchema(session.copy(logger = msg => Console.out.println(msg())), bq.ddl).unsafeRun
    bq.queries
  }

  def run[A](fa: Effect[A]): A = scala.concurrent.blocking { fa.run(session).unsafeRun }
}
