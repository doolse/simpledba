package io.doolse.simpledba.dynamodb

import io.doolse.simpledba.{Columns, Iso}
import shapeless.HList
import software.amazon.awssdk.services.dynamodb.model.{
  CreateTableRequest,
  KeySchemaElement,
  KeyType,
  ProvisionedThroughput
}

import scala.collection.JavaConverters._

case class DynamoDBTable[T, CR <: HList, Vals <: HList, PK, SK](
    name: String,
    pkColumn: (String, DynamoDBColumn[PK]),
    skColumn: Option[(String, DynamoDBColumn[SK])],
    columns: Columns[DynamoDBColumn, CR, Vals],
    iso: Iso[T, CR]) {

  def tableDefiniton: CreateTableRequest.Builder = {
    val b = CreateTableRequest.builder()
    def asKeySchema(kt: KeyType, keyCol: (String, DynamoDBColumn[_])): KeySchemaElement =
      KeySchemaElement.builder().attributeName(keyCol._1).keyType(kt).build()

    b.tableName(name)
      .attributeDefinitions(
        (Seq(pkColumn) ++ skColumn).map { case (n, c) => c.definition(n) }.asJava)
      .keySchema((Seq(asKeySchema(KeyType.HASH, pkColumn)) ++ skColumn.map(c =>
        asKeySchema(KeyType.RANGE, c))).asJavaCollection)
      .provisionedThroughput(
        ProvisionedThroughput
          .builder()
          .readCapacityUnits(1L)
          .writeCapacityUnits(1L)
          .build())

  }
}
