package io.doolse.simpledba.dynamodb

import io.doolse.simpledba.dynamodb.DynamoDBExpression.ExprState
import io.doolse.simpledba._
import shapeless.ops.record.Selector
import shapeless.{::, HList, HNil, Witness}
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, GetItemRequest, PutItemRequest, QueryRequest}

import scala.collection.JavaConverters._

object AttributeMapper extends ColumnMapper[DynamoDBColumn, String, (String, AttributeValue)] {
  override def apply[V](column: DynamoDBColumn[V], value: V, a: String): (String, AttributeValue) =
    a -> column.toAttribute(value)
}

class DynamoDBQueries[S[_], F[_]](effect: DynamoDBEffect[S, F]) {

  val S  = effect.S
  val SM = S.SM

  def writes[T](tables: DynamoDBTable.SameT[T]*): WriteQueries[S, F, T] =
    new WriteQueries[S, F, T] {
      def S = effect.S
      override def insertAll =
        ts =>
          SM.flatMap(ts) { t: T =>
            S.emits {
              tables.map { table =>
                val b = PutItemRequest.builder()
                b.tableName(table.name)
                b.item(
                  table.columns.mapRecord(table.columns.iso.to(t), AttributeMapper).toMap.asJava)
                PutItem(b.build())
              }
            }
        }

      override def updateAll = in => insertAll(SM.map(in)(_._2))

      override def deleteAll = _ => S.empty
    }

  def get(table: DynamoDBTable): GetBuilder[table.CR, table.PK :: table.SK :: HNil, table.T] =
    GetBuilder(
      table,
      pk => {
        val NamedAttribute(name, col) = table.pkColumn
        (Seq(name -> col.toAttribute(pk.head)) ++
          table.skColumn.map { sk =>
            sk.name -> sk.column.toAttribute(pk.tail.head)
          }).toMap
      },
      table.columns,
      true
    )

  def getAttr[Attrs <: HList, AttrVals <: HList](table: DynamoDBTable, attrs: Cols[Attrs])(
      implicit c: ColumnSubsetBuilder.Aux[table.CR, Attrs, AttrVals])
    : GetBuilder[AttrVals, table.PK :: table.SK :: HNil, AttrVals] =
    GetBuilder(
      table,
      pk => {
        val NamedAttribute(name, col) = table.pkColumn
        (Seq(name -> col.toAttribute(pk.head)) ++
          table.skColumn.map { sk =>
            sk.name -> sk.column.toAttribute(pk.tail.head)
          }).toMap
      },
      table.columns.subset(c)._1,
      false
    )

  def query(table: DynamoDBTable): QueryBuilder[table.T, table.CR, table.PK, table.SK] = {
    QueryBuilder(table, None)
  }

  def queryIndex(table: DynamoDBTable, indexName: Witness)(
      implicit selIndex: Selector[table.Indexes, indexName.T],
      ev: indexName.T <:< Symbol): QueryBuilder[table.T, table.CR, table.PK, Unit] = {
    val indexString = indexName.value.name
    QueryBuilder(table,
                 table.localIndexes
                   .find(_.name == indexString)
                   .asInstanceOf[Option[LocalIndex[Unit, table.CR]]])
  }

  case class QueryBuilder[T, CR <: HList, PK, SK](table: DynamoDBTable.Aux[T, CR, PK, _, _],
                                                  index: Option[LocalIndex[SK, CR]]) {

    def build[Inp](asc: Boolean)(implicit convert: AutoConvert[Inp, PK]): Inp => S[T] =
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

        SM.flatMap {
          S.evalMap(S.eval {
            effect.asyncClient
          }) { client =>
            effect.fromFuture(client.query(qb.build()))
          }
        } { response =>
          SM.flatMap(S.emits(response.items().asScala)) { attrsJ =>
            val attrs = attrsJ.asScala.toMap
            if (attrs.isEmpty) {
              S.empty
            } else {
              S.emit(table.columns.retrieve(DynamoDBRetrieve(attrs)))
            }
          }
        }

      }

  }

  case class GetBuilder[OutR <: HList, Input, Output](
      table: DynamoDBTable,
      toKeyValue: Input => Map[String, AttributeValue],
      selectCol: ColumnRecordIso[DynamoDBColumn, String, OutR, Output],
      selectAll: Boolean) {

    def build[Inp](implicit convert: AutoConvert[Inp, Input]): Inp => S[Output] = inp => {
      SM.flatMap {
        S.evalMap(
          S.eval {
              effect.asyncClient
            }) { client =>
          val b = GetItemRequest
            .builder()
            .key(toKeyValue(convert(inp)).asJava)
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
