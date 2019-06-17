package io.doolse.simpledba.dynamodb

import io.doolse.simpledba.Columns
import shapeless.{::, HList, HNil}
import software.amazon.awssdk.services.dynamodb.model._

import scala.collection.JavaConverters._

trait KeyAttribute[A] {
  def name: String
  def keyType: KeyType
  def toAttribute(a: A): AttributeValue
  def toNamedValue(a: A): (String, AttributeValue) = name -> toAttribute(a)
  def toKeySchemaElement : KeySchemaElement =
    KeySchemaElement.builder().attributeName(name).keyType(keyType).build()
  def definition: AttributeDefinition
}

object KeyAttribute {
  def unsafe[A](kt: KeyType, t: (String, DynamoDBColumn[_])) = apply[A](kt, t.asInstanceOf[(String, DynamoDBColumn[A])])

  def apply[A](kt: KeyType, t: (String, DynamoDBColumn[A])): KeyAttribute[A] = new KeyAttribute[A] {
    val col = t._2
    override def name: String = t._1
    override def keyType: KeyType = kt
    override def toAttribute(a: A): AttributeValue = col.toAttribute(a)
    override def definition: AttributeDefinition = col.definition(name)
  }

  def mapped[A, B](kt: KeyType, name0: String, col: DynamoDBColumn[A], toA: B => A): KeyAttribute[B] = new KeyAttribute[B] {
    override def name: String = name0
    override def keyType: KeyType = kt
    override def toAttribute(a: B): AttributeValue = col.toAttribute(toA(a))
    override def definition: AttributeDefinition = col.definition(name)
  }
}

case class LocalIndex[IK, CR](name: String, attribute: KeyAttribute[IK], projection: Projection)

trait DynamoDBTable {
  type T
  type CR <: HList
  type Indexes <: HList
  type PK
  type FullKey

  def name: String
  def pkColumn: KeyAttribute[PK]
  def keyColumns: Seq[KeyAttribute[_]]
  def columns: Columns[DynamoDBColumn, T, CR]
  def localIndexes: Seq[LocalIndex[_, _]]
  def derivedColumns: CR => Seq[(String, AttributeValue)]
  def keyValue: T => FullKey
  def keyAttributes: FullKey => Seq[(String, AttributeValue)]

  def tableDefiniton: CreateTableRequest.Builder = {
    val b = CreateTableRequest.builder()

    b.tableName(name)
      .attributeDefinitions((keyColumns.map(_.definition) ++ localIndexes.map(_.attribute.definition)).asJava)
      .keySchema(keyColumns.map(_.toKeySchemaElement).asJavaCollection)
      .provisionedThroughput(
        ProvisionedThroughput
          .builder()
          .readCapacityUnits(1L)
          .writeCapacityUnits(1L)
          .build())

    val indexDefs = localIndexes.map { li =>
      val sib = LocalSecondaryIndex.builder()
      sib.projection(li.projection)
      sib
        .indexName(li.name)
        .keySchema(keyColumns.head.toKeySchemaElement, li.attribute.toKeySchemaElement)
      sib.build()
    }

    if (indexDefs.nonEmpty) b.localSecondaryIndexes(indexDefs.asJavaCollection)
    b
  }

}

trait DynamoDBSortTable extends DynamoDBTable {
  type SK
  override type FullKey = PK :: SK :: HNil

  def skColumn: KeyAttribute[SK]
}

object DynamoDBTable {

  type SameT[T0] = DynamoDBTable {
    type T = T0
  }

  type FullKey[FullKey0] = DynamoDBTable {
    type FullKey = FullKey0
  }

  type PK[PK0] = DynamoDBTable {
    type PK = PK0
  }
}
