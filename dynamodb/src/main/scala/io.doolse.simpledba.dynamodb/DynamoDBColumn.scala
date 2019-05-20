package io.doolse.simpledba.dynamodb

import io.doolse.simpledba.ColumnRetrieve
import software.amazon.awssdk.services.dynamodb.model.{AttributeDefinition, AttributeValue, ScalarAttributeType}

trait DynamoDBColumn[A] {
  def toAttribute(a: A): AttributeValue
  def fromAttribute(a: Option[AttributeValue]): A
  def definition(name: String): AttributeDefinition
}

case class DynamoDBRetrieve(item: Map[String, AttributeValue]) extends ColumnRetrieve[DynamoDBColumn, String] {
  override def apply[V](column: DynamoDBColumn[V], offset: Int, a: String): V = column.fromAttribute(item.get(a))
}

class DynamoDBPKColumn[A]

object DynamoDBPKColumn
{
  implicit val stringKey = new DynamoDBPKColumn[String]
  implicit val intKey = new DynamoDBPKColumn[Int]
  implicit val longKey = new DynamoDBPKColumn[Long]
}

object DynamoDBColumn
{
  def numberColumn[A](fromString: String => A) : DynamoDBColumn[A] = new DynamoDBColumn[A] {
    override def toAttribute(a: A): AttributeValue =
      AttributeValue.builder().n(a.toString).build()


    override def fromAttribute(a: Option[AttributeValue]): A = fromString(a.get.n())

    override def definition(name: String): AttributeDefinition = {
      AttributeDefinition.builder().attributeName(name).attributeType(ScalarAttributeType.N).build()
    }
  }

  implicit def stringCol : DynamoDBColumn[String] = new DynamoDBColumn[String] {
    override def toAttribute(a: String): AttributeValue =
      AttributeValue.builder().s(a).build()


    override def fromAttribute(a: Option[AttributeValue]): String = a.get.s()

    override def definition(name: String): AttributeDefinition = {
      AttributeDefinition.builder().attributeName(name).attributeType(ScalarAttributeType.S).build()
    }
  }

  implicit def intCol : DynamoDBColumn[Int] = numberColumn[Int](_.toInt)
  implicit def longCol : DynamoDBColumn[Long] = numberColumn[Long](_.toLong)

  implicit def boolCol : DynamoDBColumn[Boolean] = new DynamoDBColumn[Boolean] {
    override def toAttribute(a: Boolean): AttributeValue =
      AttributeValue.builder().bool(a).build()


    override def fromAttribute(a: Option[AttributeValue]): Boolean = a.get.bool()

    override def definition(name: String): AttributeDefinition = {
      AttributeDefinition.builder().attributeName(name).attributeType(ScalarAttributeType.B).build()
    }
  }

}
