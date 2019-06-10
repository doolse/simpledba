package io.doolse.simpledba.dynamodb

import io.doolse.simpledba.ColumnRetrieve
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeAction,
  AttributeDefinition,
  AttributeValue,
  AttributeValueUpdate,
  ScalarAttributeType
}

trait DynamoDBColumn[A] {
  def toAttribute(a: A): AttributeValue
  def fromAttribute(a: Option[AttributeValue]): A
  def toAttributeUpdate(o: A, n: A): Option[AttributeValueUpdate]
  def definition(name: String): AttributeDefinition
}

case class DynamoDBRetrieve(item: Map[String, AttributeValue])
    extends ColumnRetrieve[DynamoDBColumn, String] {
  override def apply[V](column: DynamoDBColumn[V], offset: Int, a: String): V =
    column.fromAttribute(item.get(a))
}

class DynamoDBPKColumn[A]

object DynamoDBPKColumn {
  implicit val stringKey = new DynamoDBPKColumn[String]
  implicit val intKey    = new DynamoDBPKColumn[Int]
  implicit val longKey   = new DynamoDBPKColumn[Long]
}

case class ScalarDynamoDBColumn[A](fromAttr: AttributeValue => A,
                                   attributeType: ScalarAttributeType,
                                   toAttr: (A, AttributeValue.Builder) => Unit)
    extends DynamoDBColumn[A] {
  override def toAttribute(a: A): AttributeValue = {
    val b = AttributeValue.builder()
    toAttr(a, b)
    b.build()
  }

  override def toAttributeUpdate(o: A, n: A): Option[AttributeValueUpdate] = {
    if (o == n) None
    else {
      Some(AttributeValueUpdate.builder().action(AttributeAction.PUT).value(toAttribute(n)).build())
    }
  }

  override def fromAttribute(a: Option[AttributeValue]): A = fromAttr(a.get)

  override def definition(name: String): AttributeDefinition = {
    AttributeDefinition.builder().attributeName(name).attributeType(attributeType).build()
  }
}

object DynamoDBColumn {

  def numberColumn[A](fromString: String => A): DynamoDBColumn[A] =
    ScalarDynamoDBColumn[A](a => fromString(a.n()),
                            ScalarAttributeType.N,
                            (a, b) => b.n(a.toString))

  implicit def stringCol: DynamoDBColumn[String] =
    ScalarDynamoDBColumn[String](_.s(), ScalarAttributeType.S, (a, b) => b.s(a))
  implicit def intCol: DynamoDBColumn[Int]   = numberColumn[Int](_.toInt)
  implicit def longCol: DynamoDBColumn[Long] = numberColumn[Long](_.toLong)
  implicit def boolCol: DynamoDBColumn[Boolean] =
    ScalarDynamoDBColumn[Boolean](_.bool(), ScalarAttributeType.B, (a, b) => b.bool(a))

}
