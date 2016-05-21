package io.doolse.simpledba.dynamodb

import cats.data.{Reader, Xor}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model._
import io.doolse.simpledba.{RelationMapper, WriteQueries}
import io.doolse.simpledba.dynamodb.DynamoDBMapper.Effect
import shapeless._
import shapeless.ops.hlist
import shapeless.ops.hlist.{IsHCons, ToList}
import shapeless.ops.record._

import scala.collection.JavaConverters._

/**
  * Created by jolz on 12/05/16.
  */

object DynamoDBMapper {
  type Effect[A] = Reader[DynamoDBSession, A]

  case class DynamoDBSession(client: AmazonDynamoDBClient)

}

class DynamoDBMapper extends RelationMapper[DynamoDBMapper.Effect] {

  type PhysCol[A] = DynamoDBColumn[A]
  type DDLStatement = CreateTableRequest

  sealed trait DynamoProjection[T] extends Projection[T] {
    def materialize(mat: ColumnMaterialzer): Option[T]
  }

  case class All[T, CVL <: HList](m: ColumnMaterialzer => Option[CVL], toT: CVL => T) extends DynamoProjection[T] {
    def materialize(mat: ColumnMaterialzer): Option[T] = m(mat).map(toT)
  }

  def asAttrMap(l: List[PhysicalValue]) = l.map(physical2Attribute).toMap.asJava

  def physical2Attribute(pc: PhysicalValue) = pc.name -> pc.withCol((v, c) => c.to(v))

  sealed trait DynamoWhere {
    def forRequest: QueryRequest => QueryRequest

    def asMap: java.util.Map[String, AttributeValue]
  }

  case class QueryParam(name: String, v: PhysicalValue, op: String) {
    val colName = s"#$name" -> v.name
    val value = s":$name" -> v.withCol((a, c) => c.to(a))

    def expr = s"${colName._1} $op ${value._1}"
  }

  case class KeyMatch(pk: PhysicalValue, sk: Option[PhysicalValue]) extends DynamoWhere {
    def forRequest = qr => {
      val params = List(QueryParam("PK", pk, "=")) ++ sk.map(skv => QueryParam("SK", skv, "="))
      val expr = params.map(_.expr).mkString(" AND ")
      val nameMap = params.map(_.colName).toMap.asJava
      val valMap = params.map(_.value).toMap.asJava
      qr.withKeyConditionExpression(expr)
        .withExpressionAttributeNames(nameMap)
        .withExpressionAttributeValues(valMap)
    }

    def asMap = asAttrMap(List(pk) ++ sk)
  }

  type Where = DynamoWhere
  type ProjectionT[A] = DynamoProjection[A]

  trait DynamoPhysRelation[T] extends PhysRelation[T] {
    type CR <: HList
    type CVL <: HList
    type Meta = (CR, CVL)

    def materializer: ColumnMaterialzer => Option[CVL]

    def fromColumns: CVL => T
  }

  type PhysRelationT[T] = DynamoPhysRelation[T]

  trait DynamoDBPhysicalRelations[T, CR <: HList, CVL <: HList, PKK, SKKL] extends DepFn1[ColumnMapper.Aux[T, CR, CVL]] {
    type PKV
    type SKV
    type Out = PhysRelation.Aux[T, (CR, CVL), PKV, SKV]
  }

  object DynamoDBPhysicalRelations {

    type Aux[T, CR <: HList, CVL <: HList, PKK, SKKL, PKV0, SKV0] = DynamoDBPhysicalRelations[T, CR, CVL, PKK, SKKL] {
      type PKV = PKV0
      type SKV = SKV0
    }

    implicit def dynamoDBTable[T, CR <: HList, CVL <: HList, PKK, SKKL <: HList, PKV0, SKV0,
    SKCL <: HList, PKC]
    (implicit
     selectPkCol: Selector.Aux[CR, PKK, PKC],
     skSel: SelectAll.Aux[CR, SKKL, SKCL],
     pkv0: ColumnValuesType.Aux[PKC, PKV0],
     skv0: ColumnValuesType.Aux[SKCL, SKV0],
     skToList: ToList[SKCL, ColumnMapping[T, _]],
     evCol: PKC =:= ColumnMapping[T, PKV0],
     pkVals: PhysicalValues.Aux[PKV0, PKC, PhysicalValue],
     skVals: PhysicalValues.Aux[SKV0, SKCL, List[PhysicalValue]],
     allVals: PhysicalValues.Aux[CVL, CR, List[PhysicalValue]],
     differ: ValueDifferences.Aux[CR, CVL, CVL, List[ValueDifference]],
     materializeAll: MaterializeFromColumns.Aux[CR, CVL],
     extractVals: ValueExtractor.Aux[CR, CVL, PKK :: SKKL :: HNil, PKV0 :: SKV0 :: HNil]
    ): Aux[T, CR, CVL, PKK, SKKL, PKV0, SKV0] = new DynamoDBPhysicalRelations[T, CR, CVL, PKK, SKKL] {
      type PKV = PKV0
      type SKV = SKV0

      def apply(colMapper: ColumnMapper.Aux[T, CR, CVL]): PhysRelation.Aux[T, (CR, CVL), PKV, SKV]
      = new AbstractColumnListRelation[T, CR, CVL](colMapper) with DynamoPhysRelation[T] {
        self =>

        type PartitionKey = PKV0
        type SortKey = SKV0

        val pkCol = selectPkCol(columns)
        val skColL = skSel(columns)
        val toKeys = extractVals()

        def keysAsAttributes(keys: FullKey) =
          keyMatchFromList(keys).asMap

        def keyMatchFromList(keys: FullKey) =
          createKeyMatch(keys.head, keys.tail.head)

        def createKeyMatch(pk: PartitionKey, sk: SortKey): DynamoWhere
        = KeyMatch(pkVals(pk, pkCol), skVals(sk, skColL).headOption)

        def asValueUpdate(d: ValueDifference) = {
          d.name -> d.withCol((a, b, c) => c.diff(a, b))
        }

        def createMaterializer(m: java.util.Map[String, AttributeValue]): ColumnMaterialzer = new ColumnMaterialzer {
          def apply[A](name: String, atom: ColumnAtom[A]): Option[A] = {
            Option(m.get(name)).map(av => atom.from(atom.physicalColumn.from(av)))
          }
        }

        def whereFullKey(fk: PartitionKey :: SortKey :: HNil): DynamoWhere = keyMatchFromList(fk)

        def wherePK(pk: PartitionKey): DynamoWhere = KeyMatch(pkVals(pk, pkCol), None)

        def whereRange(pk: PKV, lower: SortKey, upper: SortKey): DynamoWhere = ???

        def projection[A](a: A)(implicit prj: Projector[T, (CR, CVL), A]): prj.Out = prj(self, a)

        def createReadQueries(tableName: String): ReadQueries = new ReadQueries {
          def selectOne[A](projection: DynamoProjection[A], where: DynamoWhere): Effect[Option[A]] = Reader { s =>
            val m = Option(s.client.getItem(tableName, where.asMap).getItem).map(createMaterializer)
            m.flatMap(projection.materialize)
          }

          def selectMany[A](projection: DynamoProjection[A], where: DynamoWhere, asc: Boolean): Effect[List[A]] = Reader { s =>
            val qr = new QueryRequest().withTableName(tableName)
            val qr2 = where.forRequest(qr).withScanIndexForward(asc)
            s.client.query(qr2).getItems.asScala.flatMap(v => projection.materialize(createMaterializer(v))).toList
          }
        }

        def createWriteQueries(tableName: String): WriteQueries[Effect, T] = new WriteQueries[Effect, T] {
          def delete(t: T): Effect[Unit] = Reader { s =>
            s.client.deleteItem(tableName, keysAsAttributes(toKeys(toColumns(t))))
          }

          def insert(t: T): Effect[Unit] = Reader { s =>
            s.client.putItem(tableName, asAttrMap(toPhysicalValues(toColumns(t))))
          }

          def update(existing: T, newValue: T): Effect[Boolean] = Reader { s =>
            changeChecker(existing, newValue).exists {
              case Xor.Left((k, changes)) =>
                s.client.updateItem(tableName, keysAsAttributes(k), changes.map(asValueUpdate).toMap.asJava)
                true
              case Xor.Right((oldKey, newk, vals)) =>
                s.client.deleteItem(tableName, keysAsAttributes(oldKey))
                s.client.putItem(tableName, asAttrMap(vals))
                true
            }
          }
        }

        def createDDL(tableName: String): CreateTableRequest = {
          val keyColumn = evCol(pkCol)
          val sortKeyList = skToList(skColL)
          val sortDef = (List(keyColumn) ++ sortKeyList).map(c => new AttributeDefinition(c.name, c.atom.physicalColumn.attributeType))
          val keyDef = new KeySchemaElement(keyColumn.name, KeyType.HASH)
          val sortKeyDef = sortKeyList.headOption.map(c => new KeySchemaElement(c.name, KeyType.RANGE))
          new CreateTableRequest(sortDef.asJava, tableName, (List(keyDef) ++ sortKeyDef).asJava, new ProvisionedThroughput(1L, 1L))
        }
      }
    }
  }

  implicit def projectAll[T, CR <: HList, CVL <: HList]: Projector.Aux[T, (CR, CVL), selectStar.type, T]
  = new Projector[T, (CR, CVL), selectStar.type] {
    type A0 = T

    def apply(t: PhysRelation.Aux[T, (CR, CVL), _, _], u: selectStar.type) = All(t.materializer, t.fromColumns)
  }

  class DynamoDBKeyMapper[T, CR <: HList, KL <: HList, CVL <: HList, PKL <: HList, PKV, SKV, PKK, SKKL]
  (implicit relMaker: DynamoDBPhysicalRelations.Aux[T, CR, CVL, PKK, SKKL, PKV, SKV])
    extends KeyMapper.Aux[T, CR, KL, CVL, PKL, (CR, CVL), PKV, SKV] {
    def keysMapped(cm: ColumnMapper.Aux[T, CR, CVL]): PhysRelation.Aux[T, (CR, CVL), PKV, SKV] = relMaker(cm)
  }

  implicit def noSortKeyMapper[T, CR <: HList, KL <: HList, CVL <: HList, PKK, PKL <: HList, PKV]
  (implicit
   pkk: IsHCons.Aux[PKL, PKK, HNil],
   ev: IsHCons.Aux[KL, PKK, HNil],
   relMaker: DynamoDBPhysicalRelations.Aux[T, CR, CVL, PKK, HNil, PKV, HNil]
  ): KeyMapper.Aux[T, CR, KL, CVL, PKL, (CR, CVL), PKV, HNil] = new DynamoDBKeyMapper

  implicit def dynamoDBRemainingSortKeyMapper[T, CR <: HList, KL <: HList, CVL <: HList,
  PKK, SKK, PKL <: HList, RemainingSK <: HList, PKV, SKV]
  (implicit
   pkk: IsHCons.Aux[PKL, PKK, HNil],
   removePK: hlist.Remove.Aux[KL, PKK, (PKK, RemainingSK)],
   skk: IsHCons.Aux[RemainingSK, SKK, HNil],
   relMaker: DynamoDBPhysicalRelations.Aux[T, CR, CVL, PKK, SKK :: HNil, PKV, SKV]
  ) : KeyMapper.Aux[T, CR, KL, CVL, PKL, (CR, CVL), PKV, SKV] = new DynamoDBKeyMapper

  implicit def dynamoDBAutoSortKeyMapper[T, CR <: HList, KL <: HList, CVL <: HList,
  PKL <: HList, PKLT <: HList, PKK, SKK, PKV, SKV]
  (implicit
   pkk: IsHCons.Aux[PKL, PKK, PKLT],
   skk: IsHCons.Aux[PKLT, SKK, HNil],
   relMaker: DynamoDBPhysicalRelations.Aux[T, CR, CVL, PKK, SKK :: HNil, PKV, SKV]
  ) : KeyMapper.Aux[T, CR, KL, CVL, PKL, (CR, CVL), PKV, SKV] = new DynamoDBKeyMapper


}
