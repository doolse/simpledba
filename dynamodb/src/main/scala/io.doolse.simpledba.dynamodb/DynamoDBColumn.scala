package io.doolse.simpledba.dynamodb

import io.doolse.simpledba.{ColumnRetrieve, Iso}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.{AttributeAction, AttributeDefinition, AttributeValue, AttributeValueUpdate, ScalarAttributeType}

trait DynamoDBColumn[A] {
  self =>
  def toAttribute(a: A): AttributeValue
  def fromAttribute(a: Option[AttributeValue]): A
  def toAttributeUpdate(o: A, n: A): Option[AttributeValueUpdate]
  def definition(name: String): AttributeDefinition
  def isoMap[B](iso: Iso[A, B]): DynamoDBColumn[B] = new DynamoDBColumn[B] {
    override def toAttribute(a: B): AttributeValue = self.toAttribute(iso.from(a))
    override def fromAttribute(a: Option[AttributeValue]): B = iso.to(self.fromAttribute(a))

    override def toAttributeUpdate(o: B, n: B): Option[AttributeValueUpdate] = self.toAttributeUpdate(iso.from(o), iso.from(n))

    override def definition(name: String): AttributeDefinition = self.definition(name)
  }
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

  def unsafeStringCol: DynamoDBColumn[String] =
    ScalarDynamoDBColumn(_.s(), ScalarAttributeType.S, (a, builder) => builder.s(a))

  implicit def safeStringCol = {
    val wrapped = optionalColumn(unsafeStringCol)
    new DynamoDBColumn[String] {
      private def noneEmpty(a: String)                    = if (a.isEmpty) None else Some(a)
      override def toAttribute(a: String): AttributeValue = wrapped.toAttribute(noneEmpty(a))

      override def fromAttribute(a: Option[AttributeValue]): String =
        wrapped.fromAttribute(a).getOrElse("")

      override def toAttributeUpdate(o: String, n: String): Option[AttributeValueUpdate] =
        wrapped.toAttributeUpdate(noneEmpty(o), noneEmpty(n))

      override def definition(name: String): AttributeDefinition = unsafeStringCol.definition(name)
    }
  }

  implicit def intCol: DynamoDBColumn[Int]   = numberColumn[Int](_.toInt)
  implicit def longCol: DynamoDBColumn[Long] = numberColumn[Long](_.toLong)

  implicit def bytesCol: DynamoDBColumn[SdkBytes] =
    ScalarDynamoDBColumn[SdkBytes](_.b(), ScalarAttributeType.B, (a, b) => b.b(a))

  implicit def boolCol: DynamoDBColumn[Boolean] =
    ScalarDynamoDBColumn[Boolean](_.bool(), ScalarAttributeType.B, (a, b) => b.bool(a))

  implicit def isoCol[A, B](implicit iso: Iso[B, A], col: DynamoDBColumn[A]): DynamoDBColumn[B] =
    new DynamoDBColumn[B] {
      def toAttribute(a: B): AttributeValue = col.toAttribute(iso.to(a))

      def fromAttribute(a: Option[AttributeValue]): B = iso.from(col.fromAttribute(a))

      def toAttributeUpdate(o: B, n: B): Option[AttributeValueUpdate] =
        col.toAttributeUpdate(iso.to(o), iso.to(n))

      def definition(name: String): AttributeDefinition = col.definition(name)
    }

  implicit def optionalColumn[A](implicit col: DynamoDBColumn[A]): DynamoDBColumn[Option[A]] =
    new DynamoDBColumn[Option[A]] {
      override def toAttribute(a: Option[A]): AttributeValue =
        a.map(col.toAttribute).getOrElse(AttributeValue.builder().nul(true).build())

      override def fromAttribute(a: Option[AttributeValue]): Option[A] =
        a.flatMap(av => if (av.nul()) None else Some(col.fromAttribute(Some(av))))

      override def toAttributeUpdate(o: Option[A], n: Option[A]): Option[AttributeValueUpdate] =
        (o, n) match {
          case (Some(oa), Some(newa)) => col.toAttributeUpdate(oa, newa)
          case (_, _) =>
            Some(
              AttributeValueUpdate
                .builder()
                .action(AttributeAction.PUT)
                .value(toAttribute(n))
                .build())
        }

      override def definition(name: String): AttributeDefinition = col.definition(name)
    }
}
