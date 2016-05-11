package test

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScalarAttributeType}
import io.doolse.simpledba.Mapper2
import io.doolse.simpledba.dynamodb.{DynamoDBColumn, DynamoDBRelationIO}
import shapeless._
import shapeless.record._
import shapeless.syntax.singleton._

/**
  * Created by jolz on 10/05/16.
  */
object TestDynamo2 extends App {


  val mapper = new Mapper2(DynamoDBRelationIO())
//  import mapper._

  case class EmbeddedFields(adminpassword: String, enabled: Boolean)
  case class Inst(uniqueid: Long, embedded: EmbeddedFields)
//  case class Inst(uniqueid: Long, adminpassword: String, enabled: Boolean)

  val instRecord = ('uniqueid ->> 0L) :: ('adminpassword ->> "hi") :: ('enabled ->> true) :: HNil
  val mappedTable = mapper.relation[Inst]("institution").compositeKey('uniqueid, 'enabled)
  val qpk = mappedTable.mapper.columns
  println(qpk)
  println(mappedTable)
}
