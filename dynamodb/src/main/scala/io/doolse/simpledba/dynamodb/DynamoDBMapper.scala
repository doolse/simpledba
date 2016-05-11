package io.doolse.simpledba.dynamodb

import com.amazonaws.services.dynamodbv2.model._
import io.doolse.simpledba._
import io.doolse.simpledba.dynamodb.DynamoDBRelationIO.{Effect, ResultOps}
import shapeless._

import scala.collection.JavaConverters._

/**
  * Created by jolz on 5/05/16.
  */

class DynamoDBMapper extends RelationMapper[Effect, ResultOps] {
  val connection = new DynamoDBRelationIO
  val resultSet = connection.rsOps

  type DDL = CreateTableRequest
  type CT[A] = DynamoDBColumn[A]

  def genDDL(physicalTable: PhysicalTable[_, _, _]): CreateTableRequest = {
    val keyList = physicalTable.columns.filter(_.meta.columnType == PartitionKey)
    val attrs = keyList.map(c => new AttributeDefinition(c.meta.name, c.column.column.attributeType))
    val keySchema = keyList.map(column => new KeySchemaElement(column.meta.name, KeyType.HASH))
    new CreateTableRequest(attrs.asJava, physicalTable.name, keySchema.asJava, new ProvisionedThroughput(1L, 1L))
  }

  implicit def keyMapper[T, TR <: HList, V <: HList, K <: HList, SK <: HList, FKV <: HList]
  (implicit fullKeyQ: KeyQueryBuilder.Aux[TR, K, FKV])
  : KeySelector.Aux[T, TR, V, (K, SK), TR, V, FKV, FKV]
  = new KeySelector[T, TR, V, (K, SK)] {
    type Out = (TR, KeyQuery[FKV], KeyQuery[FKV], V => V, V => V)

    def apply(columns: TR): Out = {
      (columns, fullKeyQ(columns), fullKeyQ(columns), identity, identity)
    }
  }


}
