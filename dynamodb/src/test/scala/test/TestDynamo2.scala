package test

import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScalarAttributeType}
import io.doolse.simpledba.Mapper2
import io.doolse.simpledba.dynamodb.{DynamoDBColumn, DynamoDBRelationIO}
import shapeless._
import shapeless.ops.record.Values
import shapeless.syntax.singleton._
import shapeless.record._
/**
  * Created by jolz on 10/05/16.
  */
object TestDynamo2 extends App {

  implicit val longColumn = DynamoDBColumn[Long](_.getN.toLong, l => new AttributeValue().withN(l.toString), ScalarAttributeType.N)
  implicit val boolColumn = DynamoDBColumn[Boolean](_.getBOOL, b => new AttributeValue().withBOOL(b), ScalarAttributeType.S)
  implicit val stringColumn = DynamoDBColumn[String](_.getS, new AttributeValue(_), ScalarAttributeType.S)

  val mapper = new Mapper2(DynamoDBRelationIO())

  case class EmbeddedFields(adminpassword: String, enabled: Boolean)
  case class Inst(uniqueid: Long, embedded: EmbeddedFields)

  val instRecord = ("uniqueid" ->> 0L) :: ("adminpassword" ->> "hi") :: ("enabled" ->> true) :: HNil
  val mappedTable = mapper.relation[Inst]("institution").compositeKey('uniqueid, 'enabled).queryByPK
  println(test.showType(mappedTable))
  println(mappedTable)
}
