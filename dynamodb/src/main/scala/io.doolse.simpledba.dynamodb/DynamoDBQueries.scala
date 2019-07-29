package io.doolse.simpledba.dynamodb

import io.doolse.simpledba.dynamodb.DynamoDBExpression.ExprState
import io.doolse.simpledba._
import shapeless.ops.record.Selector
import shapeless.{::, HList, HNil, Witness}
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  DeleteItemRequest,
  GetItemRequest,
  PutItemRequest,
  QueryRequest,
  QueryResponse,
  UpdateItemRequest
}

import scala.collection.JavaConverters._

object AttributeMapper extends ColumnMapper[DynamoDBColumn, String, (String, AttributeValue)] {
  override def apply[V](column: DynamoDBColumn[V], value: V, a: String): (String, AttributeValue) =
    a -> column.toAttribute(value)
}

class DynamoDBQueries[S[-_, _], F[-_, _], R](effect: DynamoDBEffect[S, F, R]) {

  private val S  = effect.S

  def writes[T](tables: DynamoDBTable.SameT[T]*): WriteQueries[S, F, R, DynamoDBWriteOp, T] =
    new WriteQueries[S, F, R, DynamoDBWriteOp, T] {
      def S = effect.S

      def putItem(table: DynamoDBTable)(t: table.T): DynamoDBWriteOp = {
        val b = PutItemRequest.builder()
        b.tableName(table.name)
        val itemRec = table.columns.iso.to(t)
        b.item(
          (table.columns.mapRecord(itemRec, AttributeMapper) ++ table
            .derivedColumns(t, itemRec)).toMap.asJava)
        PutItem(b.build())
      }

      override def insertAll[R1 <: R] =
        ts =>
          S.flatMapS(ts) { t: T =>
            S.emits {
              tables.map { table =>
                putItem(table)(t)
              }
            }
        }

      override def updateAll[R1 <: R] =
        in =>
          S.flatMapS(in) {
            case (o, n) =>
              tables.foldLeft(S.empty[R1, DynamoDBWriteOp])((ops, table) => {
                val b          = UpdateItemRequest.builder()
                val allColumns = table.columns
                val oldKey     = table.keyValue(o)
                val keyVal     = table.keyValue(n)
                val nextWrites = if (oldKey != keyVal) {
                  S.emits(Seq(deleteItem(table)(oldKey), putItem(table)(n)))
                } else {
                  S.empty[R1, DynamoDBWriteOp]
                }
                S.append(ops, nextWrites)
              })
        }

      def deleteItem(table: DynamoDBTable)(keys: table.FullKey): DynamoDBWriteOp = {
        val b = DeleteItemRequest.builder()
        b.tableName(table.name)
        b.key(table.keyAttributes(keys).toMap.asJava)
        DeleteItem(b.build())
      }

      override def deleteAll[R1 <: R] =
        ts =>
          S.flatMapS(ts) { t: T =>
            S.emits {
              tables.map { table =>
                deleteItem(table)(table.keyValue(t))
              }
            }

        }
    }

  def get(table: DynamoDBTable): GetBuilder[table.FullKey, table.T] =
    GetBuilder(table, table.columns, true)

  def getAttr[Attrs <: HList, AttrVals <: HList](table: DynamoDBTable, attrs: Cols[Attrs])(
      implicit c: ColumnSubsetBuilder.Aux[table.CR, Attrs, AttrVals])
    : GetBuilder[table.FullKey, AttrVals] =
    GetBuilder(
      table,
      table.columns.subset(c),
      false
    )

  def query(table: DynamoDBSortTable): QueryBuilder[table.T, table.CR, table.PK] = {
    QueryBuilder(table, table.columns, {
      val pkCol = table.pkColumn
      pk =>
        DynamoDBExpression.keyExpression(pkCol.name, pkCol.toAttribute(pk), None)
    }, None)
  }

  def queryOp(table: DynamoDBSortTable,
              sortOp: SortKeyOperator): QueryBuilder[table.T, table.CR, table.FullKey] = {
    QueryBuilder(
      table,
      table.columns, {
        val pkCol = table.pkColumn
        val skCol = table.skColumn
        k =>
          DynamoDBExpression.keyExpression(
            pkCol.name,
            pkCol.toAttribute(k.head),
            Some((skCol.name, sortOp, skCol.toAttribute(k.tail.head))))
      },
      None
    )
  }

  def queryIndex(table: DynamoDBTable, indexName: Witness)(
      implicit selIndex: Selector[table.Indexes, indexName.T],
      ev: indexName.T <:< Symbol): QueryBuilder[table.T, table.CR, table.PK] = {
    val indexString = indexName.value.name
    QueryBuilder(
      table,
      table.columns, { k =>
        val pkCol = table.pkColumn
        DynamoDBExpression.keyExpression(pkCol.name, pkCol.toAttribute(k), None)
      },
      table.localIndexes
        .find(_.name == indexString)
    )
  }

  case class QueryBuilder[T, CR <: HList, KeyInp](
      table: DynamoDBTable,
      selectCol: ColumnRetriever[DynamoDBColumn, String, T],
      keyExpr: KeyInp => DynamoDBExpression.Expr[String],
      index: Option[LocalIndex[_]]) {

    private def query(asc: Boolean, keyInp: KeyInp): QueryRequest.Builder = {
      val qb = QueryRequest.builder().tableName(table.name)
      index.foreach(li => qb.indexName(li.name))
      val (ExprState(names, vals), expr) = keyExpr(keyInp)
        .run(ExprState(Map.empty, Map.empty))
        .value
      qb.keyConditionExpression(expr)
      qb.scanIndexForward(asc)
      if (names.nonEmpty)
        qb.expressionAttributeNames(names.asJava)
      if (vals.nonEmpty) qb.expressionAttributeValues(vals.asJava)
      qb
    }

    def responseStream(qb: QueryRequest.Builder): S[R, QueryResponse] =
      S.eval {
        S.flatMapF(effect.asyncClient) { client =>
          effect.fromFuture(client.query(qb.build()))
        }
      }

    def count[Inp](implicit convert: AutoConvert[Inp, KeyInp]): Inp => S[R, Int] = inp => {
      S.mapS(responseStream(query(true, convert(inp))))(r => r.scannedCount())
    }

    def build[Inp](asc: Boolean)(implicit convert: AutoConvert[Inp, KeyInp]): Inp => S[R, T] =
      inp => {
        S.flatMapS(responseStream(query(asc, convert(inp)))) { response =>
          S.flatMapS(S.emits(response.items().asScala)) { attrsJ =>
            val attrs = attrsJ.asScala.toMap
            if (attrs.isEmpty) {
              S.empty
            } else {
              S.emit(selectCol.retrieve(DynamoDBRetrieve(attrs)))
            }
          }
        }

      }

  }

  case class GetBuilder[Input, Output](table: DynamoDBTable.FullKey[Input],
                                       selectCol: ColumnRetriever[DynamoDBColumn, String, Output],
                                       selectAll: Boolean) {

    def build[Inp](implicit convert: AutoConvert[Inp, Input]): Inp => S[R, Output] = inp => {
      S.flatMapS {
        S.evalMap(S.eval {
          effect.asyncClient
        }) { client =>
          val b = GetItemRequest
            .builder()
            .key(table.keyAttributes(convert(inp)).toMap.asJava)
            .tableName(table.name)

          if (!selectAll) {
            val prjExpr = selectCol.columns.map(c => s"#${c._1}").mkString(", ")
            b.projectionExpression(prjExpr)
            b.expressionAttributeNames(
              selectCol.columns.map(c => (s"#${c._1}", c._1)).toMap.asJava
            )
          }
          effect.fromFuture(client.getItem(b.build()))
        }
      } { response =>
        val attrs = response.item().asScala.toMap
        if (attrs.isEmpty) {
          S.empty
        } else {
          S.emit(selectCol.retrieve(DynamoDBRetrieve(attrs)))
        }
      }
    }

    def buildAs[Inp, Out](implicit convert: AutoConvert[Inp, Input],
                          convertO: AutoConvert[Output, Out]) =
      build[Inp].andThen(o => S.mapS(o)(convertO))
  }
}
