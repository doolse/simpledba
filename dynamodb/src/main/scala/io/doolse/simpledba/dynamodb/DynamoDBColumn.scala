package io.doolse.simpledba.dynamodb

import com.amazonaws.services.dynamodbv2.model._

/**
  * Created by jolz on 5/05/16.
  */

case class DynamoDBColumn[T](from: AttributeValue => T, to: T => AttributeValue,
                             attributeType: ScalarAttributeType, diff: (T, T) => AttributeValueUpdate)

object DynamoDBColumn {

  def create[T](from: AttributeValue => T, to: T => AttributeValue, attr: ScalarAttributeType)
  = DynamoDBColumn(from, to, attr, (oldV:T, newV:T) => new AttributeValueUpdate(to(newV), AttributeAction.PUT))

  implicit val longColumn = create[Long](_.getN.toLong, l => new AttributeValue().withN(l.toString), ScalarAttributeType.N)
  implicit val intColumn = create[Int](_.getN.toInt, l => new AttributeValue().withN(l.toString), ScalarAttributeType.N)
  implicit val boolColumn = create[Boolean](_.getBOOL, b => new AttributeValue().withBOOL(b), ScalarAttributeType.S)
  implicit val stringColumn = create[String](_.getS, new AttributeValue(_), ScalarAttributeType.S)
}
