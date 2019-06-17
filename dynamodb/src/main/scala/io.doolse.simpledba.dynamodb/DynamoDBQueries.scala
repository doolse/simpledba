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

class DynamoDBQueries[S[_], F[_]](effect: DynamoDBEffect[S, F]) {

  private val S  = effect.S
  private val SM = S.SM

  def writes[T](tables: DynamoDBTable.SameT[T]*): WriteQueries[S, F, T] =
    new WriteQueries[S, F, T] {
      def S = effect.S

      def putItem(table: DynamoDBTable)(t: table.T): WriteOp = {
        val b = PutItemRequest.builder()
        b.tableName(table.name)
        val itemRec = table.columns.iso.to(t)
        b.item((table.columns.mapRecord(itemRec, AttributeMapper) ++ table.derivedColumns(itemRec)).toMap.asJava)
        PutItem(b.build())
      }

      override def insertAll =
        ts =>
          SM.flatMap(ts) { t: T =>
            S.emits {
              tables.map { table =>
                putItem(table)(t)
              }
            }
        }

      override def updateAll =
        in =>
          SM.flatMap(in) {
            case (o, n) =>
              tables.foldLeft(S.empty[WriteOp])((ops, table) => {
                val b          = UpdateItemRequest.builder()
                val allColumns = table.columns
                val oldKey     = table.keyValue(o)
                val keyVal     = table.keyValue(n)
                val nextWrites = if (oldKey != keyVal) {
                  S.emits(Seq(deleteItem(table)(oldKey), putItem(table)(n)))
                } else {
                  S.empty[WriteOp]
                }
                S.append(ops, nextWrites)
              })
        }

      def deleteItem(table: DynamoDBTable)(keys: table.FullKey): WriteOp = {
        val b = DeleteItemRequest.builder()
        b.tableName(table.name)
        b.key(table.keyAttributes(keys).toMap.asJava)
        DeleteItem(b.build())
      }

      override def deleteAll =
        ts =>
          SM.flatMap(ts) { t: T =>
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
      implicit c: ColumnSubsetBuilder.Aux[table.CR, Attrs, AttrVals]): GetBuilder[table.FullKey, AttrVals] =
    GetBuilder(
      table,
      table.columns.subset(c),
      false
    )

  def query(table: DynamoDBSortTable): QueryBuilder[table.T, table.CR, table.PK, table.SK] = {
    QueryBuilder(table, table.columns, None)
  }

  def queryIndex(table: DynamoDBTable, indexName: Witness)(
      implicit selIndex: Selector[table.Indexes, indexName.T],
      ev: indexName.T <:< Symbol): QueryBuilder[table.T, table.CR, table.PK, selIndex.Out] = {
    val indexString = indexName.value.name
    QueryBuilder(table,
                 table.columns,
                 table.localIndexes
                   .find(_.name == indexString)
                   .asInstanceOf[Option[LocalIndex[selIndex.Out]]])
  }

  case class QueryBuilder[T, CR <: HList, PK, SK](table: DynamoDBTable.PK[PK],
                                              selectCol: ColumnRetriever[DynamoDBColumn, String, T],
                                              index: Option[LocalIndex[SK]]) {

    private def query(asc: Boolean, pk: PK): QueryRequest.Builder = {
      val qb = QueryRequest.builder().tableName(table.name)
      index.foreach(li => qb.indexName(li.name))
      val pkCol = table.pkColumn
      val (ExprState(names, vals), expr) = DynamoDBExpression
        .keyExpression(pkCol.name, pkCol.toAttribute(pk), None)
        .run(ExprState(Map.empty, Map.empty))
        .value
      qb.keyConditionExpression(expr)
      qb.scanIndexForward(!asc)
      if (names.nonEmpty)
        qb.expressionAttributeNames(names.asJava)
      if (vals.nonEmpty) qb.expressionAttributeValues(vals.asJava)
      qb
    }

    def responseStream(qb: QueryRequest.Builder): S[QueryResponse] =
      S.eval {
        S.M.flatMap(effect.asyncClient) { client =>
          effect.fromFuture(client.query(qb.build()))
        }
      }

    def count[Inp](implicit convert: AutoConvert[Inp, PK]): Inp => S[Int] = inp => {
      S.SM.map(responseStream(query(true, convert(inp))))(r => r.scannedCount())
    }

    def build[Inp](asc: Boolean)(implicit convert: AutoConvert[Inp, PK]): Inp => S[T] =
      inp => {
        SM.flatMap(responseStream(query(asc, convert(inp)))) { response =>
          SM.flatMap(S.emits(response.items().asScala)) { attrsJ =>
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

    def build[Inp](implicit convert: AutoConvert[Inp, Input]): Inp => S[Output] = inp => {
      SM.flatMap {
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
      build[Inp].andThen(o => SM.map(o)(convertO))
  }
}
