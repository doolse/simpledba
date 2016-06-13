package io.doolse.simpledba.dynamodb

import java.util.UUID

import com.amazonaws.services.dynamodbv2.model._

/**
  * Created by jolz on 5/05/16.
  */

case class DynamoDBColumn[T](from: AttributeValue => T, to: T => AttributeValue,
                             attributeType: ScalarAttributeType, diff: (T, T) => AttributeValueUpdate,
                             compositePart: T => String, range: (T, T))

object DynamoDBColumn {

  def create[T](from: AttributeValue => T, to: T => AttributeValue, compositePart: T => String, range: (T,T), attr: ScalarAttributeType)
  = DynamoDBColumn(from, to, attr, (oldV:T, newV:T) => new AttributeValueUpdate(to(newV), AttributeAction.PUT), compositePart, range)

  implicit val longColumn = create[Long](_.getN.toLong, l => new AttributeValue().withN(l.toString), _.toString(), (Long.MinValue, Long.MaxValue), ScalarAttributeType.N)
  implicit val intColumn = create[Int](_.getN.toInt, l => new AttributeValue().withN(l.toString), _.toString(), (Int.MinValue, Int.MaxValue), ScalarAttributeType.N)
  implicit val boolColumn = create[Boolean](_.getBOOL, b => new AttributeValue().withBOOL(b), _.toString(), (false, true), ScalarAttributeType.S)
  implicit val stringColumn = create[String](_.getS, new AttributeValue(_), identity, ("A", "Z"), ScalarAttributeType.S)
  implicit val uuidColumn = create[UUID](v => UUID.fromString(v.getS), u => new AttributeValue(u.toString), _.toString(),
    (new UUID(0L, 0L), new UUID(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL)), ScalarAttributeType.S)
}
