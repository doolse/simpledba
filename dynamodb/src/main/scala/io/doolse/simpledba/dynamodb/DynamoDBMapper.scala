package io.doolse.simpledba.dynamodb

import cats.data.{Reader, Xor}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model._
import io.doolse.simpledba._
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

  type ColumnAtom[A] = DynamoDBColumn[A]
  type DDLStatement = CreateTableRequest

  sealed trait DynamoProjection[T] {
    def materialize: ColumnMaterialzer => Option[T]
  }

  case class All[T, CVL <: HList](materialize: ColumnMaterialzer => Option[T]) extends DynamoProjection[T]

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
  type Projection[A] = DynamoProjection[A]
  type KeyMapperT = DynamoDBKeyMapper

  trait DynamoDBPhysicalRelations[T, CR <: HList, CVL <: HList, PKN0, SKN0, PKV, SKV] extends DepFn2[ColumnMapper[T, CR, CVL], String] {
    type Out = PhysRelationImpl[T, PKV, SKV]
  }

  object DynamoDBPhysicalRelations {

    implicit def dynamoDBTable[T, CR <: HList, CVL <: HList, PKK, SKKL <: HList, PKV, SKV,
    SKCL <: HList, PKC]
    (implicit
     selectPkCol: Selector.Aux[CR, PKK, PKC],
     skSel: SelectAll.Aux[CR, SKKL, SKCL],
     pkValB: PhysicalValues.Aux[PKC, PKV, PhysicalValue],
     skToList: ToList[SKCL, ColumnMapping[T, _]],
     evCol: PKC =:= ColumnMapping[T, PKV],
     skValB: PhysicalValues.Aux[SKCL, SKV, List[PhysicalValue]],
     helperB: ColumnListHelperBuilder[T, CR, CVL, PKV :: SKV :: HNil],
     extractVals: ValueExtractor.Aux[CR, CVL, PKK :: SKKL :: HNil, PKV :: SKV :: HNil]
    ) = new DynamoDBPhysicalRelations[T, CR, CVL, PKK, SKKL, PKV, SKV] {

      def apply(colMapper: ColumnMapper[T, CR, CVL], tableName: String): PhysRelationImpl[T, PKV, SKV]
      = new PhysRelationImpl[T, PKV, SKV] {
        self =>
        val toKeys = extractVals()
        val helper = helperB(colMapper, toKeys)
        val pkCol = selectPkCol(colMapper.columns)
        val skColL = skSel(colMapper.columns)
        val pkVals = pkValB(pkCol)
        val skVals = skValB(skColL)

        def keysAsAttributes(keys: FullKey) =
          keyMatchFromList(keys).asMap

        def keyMatchFromList(keys: FullKey) =
          createKeyMatch(keys.head, keys.tail.head)

        def createKeyMatch(pk: PartitionKey, sk: SortKey): DynamoWhere
        = KeyMatch(pkVals(pk), skVals(sk).headOption)

        def asValueUpdate(d: ValueDifference) = {
          d.name -> d.withCol((a, b, c) => c.diff(a, b))
        }

        def createMaterializer(m: java.util.Map[String, AttributeValue]): ColumnMaterialzer = new ColumnMaterialzer {
          def apply[A](name: String, atom: DynamoDBColumn[A]): Option[A] = {
            Option(m.get(name)).map(av => atom.from(av))
          }
        }

        def whereFullKey(fk: PartitionKey :: SortKey :: HNil): DynamoWhere = keyMatchFromList(fk)

        def wherePK(pk: PartitionKey): DynamoWhere = KeyMatch(pkVals(pk), None)

        def whereRange(pk: PKV, lower: SortKey, upper: SortKey): DynamoWhere = ???

        def projection[A](a: A)(implicit prj: Projector[T, (CR, CVL), A]): prj.Out = prj(self, a)

        def createReadQueries: ReadQueries = new ReadQueries {
          def selectOne[A](projection: DynamoProjection[A], where: DynamoWhere): Effect[Option[A]] = Reader { s =>
            val m = Option(s.client.getItem(tableName, where.asMap).getItem).map(createMaterializer)
            m.flatMap(projection.materialize)
          }

          def selectMany[A](projection: DynamoProjection[A], where: DynamoWhere, asc: Option[Boolean]): Effect[List[A]] = Reader { s =>
            val qr = where.forRequest(new QueryRequest().withTableName(tableName))
            val qr2 = asc.map(a => qr.withScanIndexForward(a)).getOrElse(qr)
            s.client.query(qr2).getItems.asScala.flatMap(v => projection.materialize(createMaterializer(v))).toList
          }
        }

        def createWriteQueries: WriteQueries[Effect, T] = new WriteQueries[Effect, T] {
          def delete(t: T): Effect[Unit] = Reader { s =>
            s.client.deleteItem(tableName, keysAsAttributes(toKeys(colMapper.toColumns(t))))
          }

          def insert(t: T): Effect[Unit] = Reader { s =>
            s.client.putItem(tableName, asAttrMap(helper.toPhysicalValues(t)))
          }

          def update(existing: T, newValue: T): Effect[Boolean] = Reader { s =>
            helper.changeChecker(existing, newValue).exists {
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

        def createDDL: CreateTableRequest = {
          val keyColumn = evCol(pkCol)
          val sortKeyList = skToList(skColL)
          val sortDef = (List(keyColumn) ++ sortKeyList).map(c => new AttributeDefinition(c.name, c.atom.attributeType))
          val keyDef = new KeySchemaElement(keyColumn.name, KeyType.HASH)
          val sortKeyDef = sortKeyList.headOption.map(c => new KeySchemaElement(c.name, KeyType.RANGE))
          new CreateTableRequest(sortDef.asJava, tableName, (List(keyDef) ++ sortKeyDef).asJava, new ProvisionedThroughput(1L, 1L))
        }

        def selectAll: DynamoProjection[T] = All(helper.materializer)
      }
    }
  }

  trait DynamoDBKeyMapper

  object DynamoDBKeyMapper {
    def apply[T, CR <: HList, KL <: HList, CVL <: HList, Q, PKV, SKV, PKK, SKKL]
    (relMaker: DynamoDBPhysicalRelations[T, CR, CVL, PKK, SKKL, PKV, SKV]): KeyMapper.Aux[T, CR, KL, CVL, Q, PKK, PKV, SKKL, SKV]
    = new DynamoDBKeyMapper with KeyMapper[T, CR, KL, CVL, Q] {
      type PartitionKey = PKV
      type SortKey = SKV
      type PartitionKeyNames = PKK
      type SortKeyNames = SKKL

      def keysMapped(cm: ColumnMapper[T, CR, CVL])(name: String): PhysRelationImpl[T, PKV, SKV] = relMaker(cm, name)
    }

    implicit def primaryKey[K, T, CR <: HList, CVL <: HList, PKK, PKV]
    (implicit
     relMaker: DynamoDBPhysicalRelations[T, CR, CVL, PKK, HNil, PKV, HNil]
    ) : KeyMapper.Aux[T, CR, PKK :: HNil, CVL, QueryUnique[K, HNil], PKK, PKV, HNil, HNil]
    = DynamoDBKeyMapper(relMaker)

    implicit def twoKeys[K, T, CR <: HList, CVL <: HList, PKK, PKV, SKK, SKV]
    (implicit
     relMaker: DynamoDBPhysicalRelations[T, CR, CVL, PKK, SKK :: HNil, PKV, SKV]
    ) : KeyMapper.Aux[T, CR, PKK :: SKK :: HNil, CVL, QueryUnique[K, HNil], PKK, PKV, SKK :: HNil, SKV]
    = DynamoDBKeyMapper(relMaker)

    implicit def multiNoSort[K, T, CR <: HList, CVL <: HList, KL <: HList, PKK, PKV, SKL, SKV]
    (implicit
     remPK: hlist.Remove.Aux[KL, PKK, (PKK, SKL)],
     relMaker: DynamoDBPhysicalRelations[T, CR, CVL, PKK, SKL, PKV, SKV]
    ) : KeyMapper.Aux[T, CR, KL, CVL, QueryMultiple[K, PKK :: HNil, HNil], PKK, PKV, SKL, SKV]
    = DynamoDBKeyMapper(relMaker)

    //    implicit def noSortKeyMapper[T, CR <: HList, KL <: HList, CVL <: HList, K, PKL <: HList, PKK, PKV]
//    (implicit
//     pkk: IsHCons.Aux[PKL, PKK, HNil],
//     ev: IsHCons.Aux[KL, PKK, HNil],
//     relMaker: DynamoDBPhysicalRelations[T, CR, CVL, PKK, HNil, PKV, HNil]
//    ): KeyMapper.Aux[T, CR, KL, CVL, QueryUnique[K, PKL], PKK, PKV, HNil, HNil] = DynamoDBKeyMapper(relMaker)
//
//    implicit def dynamoDBRemainingSortKeyMapper[T, CR <: HList, KL <: HList, CVL <: HList,
//    PKK, SKK, K, PKL <: HList, RemainingSK <: HList, PKV, SKV]
//    (implicit
//     pkk: IsHCons.Aux[PKL, PKK, HNil],
//     removePK: hlist.Remove.Aux[KL, PKK, (PKK, RemainingSK)],
//     skk: IsHCons.Aux[RemainingSK, SKK, HNil],
//     relMaker: DynamoDBPhysicalRelations[T, CR, CVL, PKK, SKK :: HNil, PKV, SKV]
//    ): KeyMapper.Aux[T, CR, KL, CVL, QueryMultiple[K, PKL, HNil], PKK, PKV, SKK :: HNil, SKV] = DynamoDBKeyMapper(relMaker)
//
//    implicit def dynamoDBAutoSortKeyMapper[T, CR <: HList, KL <: HList, CVL <: HList, K,
//    PKL <: HList, PKLT <: HList, PKK, SKK, PKV, SKV]
//    (implicit
//     pkk: IsHCons.Aux[PKL, PKK, PKLT],
//     skk: IsHCons.Aux[PKLT, SKK, HNil],
//     relMaker: DynamoDBPhysicalRelations[T, CR, CVL, PKK, SKK :: HNil, PKV, SKV]
//    ): KeyMapper.Aux[T, CR, KL, CVL, QueryMultiple[K, PKL, HNil], PKK, PKV, SKK :: HNil, SKV] = DynamoDBKeyMapper(relMaker)
  }

  def doWrapAtom[S, A](atom: DynamoDBColumn[A], to: (S) => A, from: (A) => S): DynamoDBColumn[S] = DynamoDBColumn[S](
    atom.from andThen from,
    atom.to compose to,
    atom.attributeType, (ex, nv) => atom.diff(to(ex), to(nv)))
}
