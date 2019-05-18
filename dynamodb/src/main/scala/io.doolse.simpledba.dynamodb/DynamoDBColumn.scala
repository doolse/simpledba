package io.doolse.simpledba.dynamodb

import software.amazon.awssdk.services.dynamodb.model.{AttributeDefinition, AttributeValue, ScalarAttributeType}

trait DynamoDBColumn[A] {
  def toAttribute(a: A): Option[AttributeValue]
  def fromAttribute(a: Option[AttributeValue]): A
  def definition(name: String): AttributeDefinition
}

class DynamoDBPKColumn[A]

object DynamoDBPKColumn
{
  implicit val stringKey = new DynamoDBPKColumn[String]
  implicit val intKey = new DynamoDBPKColumn[Int]
}

object DynamoDBColumn
{
  implicit def stringCol : DynamoDBColumn[String] = new DynamoDBColumn[String] {
    override def toAttribute(a: String): Option[AttributeValue] =
      Some(AttributeValue.builder().s(a).build())


    override def fromAttribute(a: Option[AttributeValue]): String = a.get.s()

    override def definition(name: String): AttributeDefinition = {
      AttributeDefinition.builder().attributeName(name).attributeType(ScalarAttributeType.S).build()
    }
  }

  implicit def intCol : DynamoDBColumn[Int] = new DynamoDBColumn[Int] {
    override def toAttribute(a: Int): Option[AttributeValue] =
      Some(AttributeValue.builder().n(a.toString).build())


    override def fromAttribute(a: Option[AttributeValue]): Int = a.get.n().toInt

    override def definition(name: String): AttributeDefinition = {
      AttributeDefinition.builder().attributeName(name).attributeType(ScalarAttributeType.N).build()
    }
  }
}
