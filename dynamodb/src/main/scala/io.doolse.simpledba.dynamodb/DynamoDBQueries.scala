package io.doolse.simpledba.dynamodb

import fs2.{Pipe, Stream}
import io.doolse.simpledba.dynamodb.DynamoDBExpression.ExprState
import io.doolse.simpledba.{
  AutoConvert,
  ColumnMapper,
  ColumnRetrieve,
  Columns,
  WriteOp,
  WriteQueries
}
import shapeless.labelled.FieldType
import shapeless.ops.record.Selector
import shapeless.{HList, Witness}
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  GetItemRequest,
  PutItemRequest,
  QueryRequest
}

import scala.collection.JavaConverters._

object AttributeMapper extends ColumnMapper[DynamoDBColumn, String, (String, AttributeValue)] {
  override def apply[V](column: DynamoDBColumn[V], value: V, a: String): (String, AttributeValue) =
    a -> column.toAttribute(value)
}

class DynamoDBQueries[F[_]](effect: DynamoDBEffect[F]) {

  def writes[T](tables: DynamoDBTable.SameT[T]*): WriteQueries[F, T] = new WriteQueries[F, T] {
    override def insertAll: Pipe[F, T, WriteOp] = _.flatMap { t: T =>
      Stream.emits {
        tables.map { table =>
          val cr = table.iso.to(t)
          val b  = PutItemRequest.builder()
          b.tableName(table.name)
          b.item(table.columns.mapRecord(table.columns.iso.to(cr), AttributeMapper).toMap.asJava)
          PutItem(b.build())
        }
      }
    }

    override def updateAll: Pipe[F, (T, T), WriteOp] = in => insertAll(in.map(_._2))

    override def deleteAll: Pipe[F, T, WriteOp] = _ => Stream.empty
  }

  def get(table: DynamoDBTable.NoSortKey): GetBuilder[table.T, table.CR, table.Vals, table.PK] =
    GetBuilder(table, pk => {
      val NamedAttribute(name, col) = table.pkColumn
      Map(name -> col.toAttribute(pk))
    })

  def query(
      table: DynamoDBTable): QueryBuilder[table.T, table.CR, table.Vals, table.PK, table.SK] = {
    QueryBuilder(table, None)
  }

  def queryIndex(table: DynamoDBTable, indexName: Witness)(
      implicit selIndex: Selector[table.Indexes, indexName.T],
      ev: indexName.T <:< Symbol)
    : QueryBuilder[table.T, table.CR, table.Vals, table.PK, Unit] = {
    val indexString = indexName.value.name
    QueryBuilder(table,
                 table.localIndexes
                   .find(_.name == indexString)
                   .asInstanceOf[Option[LocalIndex[Unit, table.CR]]])
  }

  case class QueryBuilder[T, CR <: HList, Vals <: HList, PK, SK](
      table: DynamoDBTable.Aux[T, CR, Vals, PK, _, _],
      index: Option[LocalIndex[SK, CR]]) {

    def build[Inp](asc: Boolean)(implicit convert: AutoConvert[Inp, PK]): Inp => Stream[F, T] =
      inp => {
        val qb = QueryRequest.builder().tableName(table.name)
        index.foreach(li => qb.indexName(li.name))
        val pkCol = table.pkColumn
        val (ExprState(names, vals), expr) = DynamoDBExpression
          .keyExpression(pkCol.name, pkCol.column.toAttribute(convert(inp)), None)
          .run(ExprState(Map.empty, Map.empty))
          .value
        qb.keyConditionExpression(expr)
        qb.scanIndexForward(!asc)
        if (names.nonEmpty)
          qb.expressionAttributeNames(names.asJava)
        if (vals.nonEmpty) qb.expressionAttributeValues(vals.asJava)

        Stream
          .eval {
            effect.asyncClient
          }
          .evalMap { client =>
            effect.fromFuture(client.query(qb.build()))
          }
          .flatMap { response =>
            Stream.emits(response.items().asScala).flatMap { attrsJ =>
              val attrs = attrsJ.asScala.toMap
              if (attrs.isEmpty) {
                Stream.empty
              } else {
                Stream.emit(table.iso.from(
                  table.columns.iso.from(table.columns.mkRecord(DynamoDBRetrieve(attrs)))))
              }
            }
          }

      }

  }

  case class GetBuilder[T, CR <: HList, Vals <: HList, Input](
      table: DynamoDBTable.Aux[T, CR, Vals, _, _, _],
      toKeyValue: Input => Map[String, AttributeValue]) {
    def build[Inp](implicit convert: AutoConvert[Inp, Input]): Inp => Stream[F, T] = inp => {
      Stream
        .eval {
          effect.asyncClient
        }
        .evalMap { client =>
          val b = GetItemRequest.builder().key(toKeyValue(inp).asJava).tableName(table.name)
          effect.fromFuture(client.getItem(b.build()))
        }
        .flatMap { response =>
          val attrs = response.item().asScala.toMap
          if (attrs.isEmpty) {
            Stream.empty
          } else {
            Stream.emit(
              table.iso.from(
                table.columns.iso.from(table.columns.mkRecord(DynamoDBRetrieve(attrs)))))
          }

        }
    }
  }
}
