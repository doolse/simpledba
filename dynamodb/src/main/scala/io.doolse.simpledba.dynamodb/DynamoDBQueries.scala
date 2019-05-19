package io.doolse.simpledba.dynamodb

import fs2.Pipe
import io.doolse.simpledba.{ColumnMapper, WriteOp, WriteQueries}
import shapeless.HList
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, GetItemRequest, PutItemRequest}

import scala.collection.JavaConverters._

object AttributeMapper extends ColumnMapper[DynamoDBColumn, String, (String, AttributeValue)] {
  override def apply[V](column: DynamoDBColumn[V], value: V, a: String): (String, AttributeValue) =
    a -> column.toAttribute(value)
}

class DynamoDBQueries[F[_]] {

  def writes[T, CR <: HList, Vals <: HList](table: DynamoDBTable[T, CR, Vals, _, _]): WriteQueries[F, T] =
    new WriteQueries[F, T] {
      override def insertAll: Pipe[F, T, WriteOp] = _.map { t =>
        val cr = table.iso.to(t)
        val b  = PutItemRequest.builder()
        b.tableName(table.name)
        b.item(table.columns.mapRecord(table.columns.iso.to(cr), AttributeMapper).toMap.asJava)
        PutItem(b.build())
      }

      override def updateAll: Pipe[F, (T, T), WriteOp] = ???

      override def deleteAll: Pipe[F, T, WriteOp] = ???
    }

  def get[T, CR <: HList, Vals <: HList, PK](table: DynamoDBTable[T, CR, Vals, PK, Nothing]):
    PK => GetItemRequest.Builder = pk => {
    val b = GetItemRequest.builder()
    val (name, col) = table.pkColumn
    b.key(Map(name -> col.toAttribute(pk)).asJava)
    b
  }

  class GetBuilder[T](fromItem: java.util.Map[String, AttributeValue] => T) {

  }
}
