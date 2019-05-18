package io.doolse.simpledba.dynamodb

import io.doolse.simpledba.Columns
import shapeless.HList
import software.amazon.awssdk.services.dynamodb.model.{
  CreateTableRequest,
  KeySchemaElement,
  KeyType
}

import scala.collection.JavaConverters._

case class DynamoDBTable[T, Repr <: HList, All <: HList, PK, SK](
    name: String,
    pkColumn: (String, DynamoDBColumn[PK]),
    skColumn: Option[(String, DynamoDBColumn[SK])],
    columns: Columns[DynamoDBColumn, All, Repr]) {

  def tableDefiniton: CreateTableRequest.Builder = {
    val b    = CreateTableRequest.builder()
    def defs = {}
    def asKeySchema(kt: KeyType, keyCol: (String, DynamoDBColumn[_])) = keyCol match {
      case (kname, c) => KeySchemaElement.builder().attributeName(kname).keyType(kt).build()
    }

    b.tableName(name)
      .attributeDefinitions(
        (Seq(pkColumn) ++ skColumn).map { case (n, c) => c.definition(n) }.asJava)
      .keySchema((Seq(asKeySchema(KeyType.HASH, pkColumn)) ++ skColumn.map(c =>
        asKeySchema(KeyType.RANGE, c))).asJavaCollection)
  }
}
