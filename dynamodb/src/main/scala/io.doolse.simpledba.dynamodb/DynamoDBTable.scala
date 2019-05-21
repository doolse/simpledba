package io.doolse.simpledba.dynamodb

import io.doolse.simpledba.dynamodb.DynamoDBTable.Aux
import io.doolse.simpledba.{Columns, Iso}
import shapeless.labelled.FieldType
import shapeless.ops.record.Selector
import shapeless.{::, HList, Witness}
import software.amazon.awssdk.services.dynamodb.model.{
  CreateTableRequest,
  KeySchemaElement,
  KeyType,
  LocalSecondaryIndex,
  Projection,
  ProvisionedThroughput
}

import scala.collection.JavaConverters._

case class NamedAttribute[A](name: String, column: DynamoDBColumn[A])

object NamedAttribute
{
  def unsafe[A](t: (String, DynamoDBColumn[_])): NamedAttribute[A] = NamedAttribute(t._1, t._2.asInstanceOf[DynamoDBColumn[A]])
}

case class LocalIndex[IK, CR](name: String, attribute: NamedAttribute[IK], projection: Projection)

trait DynamoDBTable {
  type T
  type CR <: HList
  type PK
  type SK
  type Indexes <: HList

  def name: String
  def pkColumn: NamedAttribute[PK]
  def skColumn: Option[NamedAttribute[SK]]
  def columns: Columns[DynamoDBColumn, T, CR]
  def localIndexes: Seq[LocalIndex[_, _]]

  protected def addIndex[NewIndexes <: HList](
      index: LocalIndex[_, _]): DynamoDBTable.Aux[T, CR, PK, SK, NewIndexes]

  def withLocalIndex[S <: Symbol, CS <: Symbol, IK](name: Witness.Aux[S], column: Witness.Aux[CS])(
      implicit kc: Selector.Aux[CR, column.T, IK],
      dynamoDBColumn: DynamoDBColumn[IK])
    : DynamoDBTable.Aux[T, CR, PK, SK, FieldType[S, LocalIndex[IK, CR]] :: Indexes] =
    addIndex(
      LocalIndex(name.value.name,
                 NamedAttribute(column.value.name, dynamoDBColumn),
                 Projection.builder().projectionType("ALL").build))

  def tableDefiniton: CreateTableRequest.Builder = {
    val b = CreateTableRequest.builder()
    def asKeySchema(kt: KeyType, keyCol: NamedAttribute[_]): KeySchemaElement =
      KeySchemaElement.builder().attributeName(keyCol.name).keyType(kt).build()

    val partitionKeySchema = asKeySchema(KeyType.HASH, pkColumn)
    b.tableName(name)
      .attributeDefinitions((Seq(pkColumn) ++ localIndexes.map(_.attribute) ++ skColumn).map {
        case NamedAttribute(n, c) => c.definition(n)
      }.asJava)
      .keySchema((Seq(partitionKeySchema) ++ skColumn
        .map(c => asKeySchema(KeyType.RANGE, c))).asJavaCollection)
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
        .keySchema(partitionKeySchema, asKeySchema(KeyType.RANGE, li.attribute))
      sib.build()
    }

    if (indexDefs.nonEmpty) b.localSecondaryIndexes(indexDefs.asJavaCollection)
    b
  }
}

case class DynamoDBTableRepr[T0, CR0 <: HList, PK0, SK0, Indexes0 <: HList](
    name: String,
    pkColumn: NamedAttribute[PK0],
    skColumn: Option[NamedAttribute[SK0]],
    columns: Columns[DynamoDBColumn, T0, CR0],
    localIndexes: Seq[LocalIndex[_, _]])
    extends DynamoDBTable {
  type T       = T0
  type CR      = CR0
  type PK      = PK0
  type SK      = SK0
  type Indexes = Indexes0

  override protected def addIndex[NewIndexes <: HList](index: LocalIndex[_, _]) =
    copy(localIndexes = index +: localIndexes)
}

object DynamoDBTable {

  type SameT[T0]  = DynamoDBTable {
    type T = T0
  }

  type Aux[T0, CR0, PK0, SK0, Indexes0] = DynamoDBTable {
    type T       = T0
    type CR      = CR0
    type PK      = PK0
    type SK      = SK0
    type Indexes = Indexes0
  }
}
