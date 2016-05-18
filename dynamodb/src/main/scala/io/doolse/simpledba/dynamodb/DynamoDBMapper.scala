package io.doolse.simpledba.dynamodb

import cats.{Eval, Id, Monad}
import cats.data.{Reader, State, Xor}
import com.amazonaws.services.dynamodbv2.model._
import io.doolse.simpledba.dynamodb.DynamoDBRelationIO.{Effect, ResultOps}
import io.doolse.simpledba.{ColumnName, RelationMapper}
import shapeless._
import shapeless.labelled.FieldType
import shapeless.ops.hlist.{IsHCons, ToList, Zip, ZipOne, ZipWith, ZipWithKeys}
import shapeless.ops.record._
import cats.syntax.all._
import shapeless.ops.hlist

import scala.collection.JavaConverters._

/**
  * Created by jolz on 12/05/16.
  */


class DynamoDBMapper extends RelationMapper[DynamoDBRelationIO.Effect] {

  type PhysCol[A] = DynamoDBColumn[A]
  type DDLStatement = CreateTableRequest

  sealed trait DynamoProjection[T] extends Projection[T] {
    def materialize(mat: ColumnMaterialzer): Option[T]
  }
  case class All[T, CVL <: HList](m: ColumnMaterialzer => Option[CVL], toT: CVL => T) extends DynamoProjection[T] {
    def materialize(mat: ColumnMaterialzer): Option[T] = m(mat).map(toT)
  }

  sealed trait DynamoWhere
  case class AttributeMatch(matches: List[(String, AttributeValue)]) extends DynamoWhere

  type Where = DynamoWhere
  type ProjectionT[A] = DynamoProjection[A]

  sealed trait DynamoPhysRelation[T] extends PhysRelation[T] {
    type CR <: HList
    type CVL <: HList
    type Meta = (CR,CVL)
    def mapper: ColumnMapper.Aux[T, CR, CVL]
    def materializer: ColumnMaterialzer => Option[CVL]
  }

  type PhysRelationT[T] = DynamoPhysRelation[T]

  implicit def projectAll[T, CR <: HList, CVL <: HList] : Projector.Aux[T, (CR, CVL), selectStar.type, T] = new Projector[T, (CR, CVL), selectStar.type] {
    type A0 = T

    def apply(t: PhysRelation.Aux[T, (CR, CVL), _, _], u: selectStar.type) = All(t.materializer, t.mapper.fromColumns)
  }

  implicit def noSortKeyMapper[T, CR <: HList, KL <: HList, CVL <: HList,
  PKK, PKL <: HList, PKC, PKV, PZL <: HList
  ]
  (implicit
   pkk: IsHCons.Aux[PKL, PKK, HNil],
   ev: IsHCons.Aux[KL, PKK, HNil],
   pkColumn: Selector.Aux[CR, PKK, PKC],
   valType: ColumnValuesType.Aux[PKC, PKV],
   evCol: PKC =:= ColumnMapping[T, PKV],
   pVals: PhysicalValues[CVL, CR],
   pkVals: PhysicalValues[PKV, PKC],
   mater: MaterializeFromColumns.Aux[CR, CVL],
   extractVals: ValueExtractor.Aux[CR, CVL, PKK :: HNil :: HNil, PKV :: HNil :: HNil]
  )
  : KeyMapper.Aux[T, CR, KL, CVL, PKL, (CR, CVL), PKV, HNil] = new KeyMapper[T, CR, KL, CVL, PKL] {
    type Meta = (CR, CVL)
    type PartitionKey = PKV
    type SortKey = HNil

    def apply(t: RelationBuilder.Aux[T, CR, KL, CVL]) = {
      DynamoDBPhysicalRelation(t.mapper, pkColumn(t.mapper.columns), HNil : HNil, extractVals())
    }
  }

  implicit def dynamoDBRemainingSortKeyMapper[T, CR <: HList, KL <: HList, CVL <: HList,
  PKK, SKK, KeysOnlyR <: HList,
  PKL <: HList, KLT <: HList, RemainingSK <: HList, PKC, SKC, PKV, SKV
  ]
  (implicit
   pkk: IsHCons.Aux[PKL, PKK, HNil],
   removePK: hlist.Remove.Aux[KL, PKK, (PKK, RemainingSK)],
   skk: IsHCons.Aux[RemainingSK, SKK, HNil],
   pkColumn: Selector.Aux[CR, PKK, PKC],
   pkv: ColumnValuesType.Aux[PKC, PKV],
   skColumn: Selector.Aux[CR, SKK, SKC],
   skv: ColumnValuesType.Aux[SKC, SKV],
   ev: PKC =:= ColumnMapping[T, PKV],
   ev2: SKC =:= ColumnMapping[T, SKV],
   pVals: PhysicalValues[CVL, CR],
   pkVals: PhysicalValues[PKV, PKC],
   mater: MaterializeFromColumns.Aux[CR, CVL],
   extractVals: ValueExtractor.Aux[CR, CVL, PKK :: SKK :: HNil, PKV :: SKV :: HNil]
  )
  : KeyMapper.Aux[T, CR, KL, CVL, PKL, (CR, CVL), PKV, SKV] = new KeyMapper[T, CR, KL, CVL, PKL] {
    type Meta = (CR, CVL)
    type PartitionKey = PKV
    type SortKey = SKV
    def apply(t: RelationBuilder.Aux[T, CR, KL, CVL]) = {
      val cols = t.mapper.columns
      DynamoDBPhysicalRelation(t.mapper, pkColumn(cols), ev2(skColumn(cols)), extractVals())
    }
  }

  implicit def dynamoDBAutoSortKeyMapper[T, CR <: HList, KL <: HList, CVL <: HList,
  PKK, SKK, KeysOnlyR <: HList,
  PKL <: HList, PKLT <: HList
  ]
  (implicit
   pkk: IsHCons.Aux[PKL, PKK, PKLT],
   skk: IsHCons.Aux[PKLT, SKK, HNil],
   keysOnly: SelectAll.Aux[CR, PKK :: SKK :: HNil, KeysOnlyR])

  : KeyMapper.Aux[T, CR, KL, CVL, PKL, (CR, CVL), Nothing, Nothing] = ???

  //  = new KeyMapper[T, CR, KL, CVL, PKK :: SKK :: HNil] {
  //    type Meta = Nothing
  //    type PartitionKey = Nothing
  //    type SortKey = Nothing
  //
  //    def apply(t: RelationBuilder.Aux[T, CR, KL, CVL]): PhysRelation.Aux[T, Nothing, Nothing, Nothing] = ???
  //  }

  //  case class KeyMapperData[T, Key, SortKey, PKeyValues, SKValues](makeTable: String => DynamoDBTable[T, Key, SortKey],
  //                                                       makeQuery: DynamoDBTable[T, Key, SortKey] => RelationQueries.Aux[T, PKeyValues, SKValues])
  //
  //  trait KeyMapper[T, ColumnsIn <: HList, AllKeys <: HList, ColumnsValues <: HList, Keys, PKeyValues, SKeyValues] {
  //    type ColumnsOut <: HList
  //    type Key
  //    type SortKey
  //
  //    def table(builder: RelationBuilder.Aux[T, ColumnsIn, AllKeys, ColumnsValues]): KeyMapperData[T, Key, SortKey, PKeyValues, SKeyValues]
  //  }
  //
  //  implicit def singleKeyMapper[T, ColumnsIn <: HList, ColumnsValues <: HList, KeyName <: Symbol, KeyColumn <: HList, KeyValue,
  //  AllColumnKeys <: HList, CVRecord <: HList]
  //  (implicit keyName: Witness.Aux[KeyName],
  //   selectedKeys: SelectAll.Aux[ColumnsIn, KeyName :: HNil, ColumnMapping[T, KeyValue] :: HNil],
  //   allColumnKeys: Keys.Aux[ColumnsIn, AllColumnKeys],
  //   allValues: ToQueryParameters.Aux[ColumnsIn, ColumnsValues],
  //   valuesToRecord: ZipWithKeys.Aux[AllColumnKeys, ColumnsValues, CVRecord],
  //   keyValue: Selector.Aux[CVRecord, KeyName, KeyValue],
  //   allColumnNames: ColumnNames[ColumnsIn],
  //   colsToRS: ColumnsAsRS.Aux[ColumnsIn, ColumnsValues])
  //  = new KeyMapper[T, ColumnsIn, KeyName :: HNil, ColumnsValues, KeyName :: HNil, KeyValue :: HNil] {
  //    type ColumnsOut = ColumnsIn
  //    type SortKey = Unit
  //    type Key = KeyValue
  //
  //    def table(builder: RelationBuilder.Aux[T, ColumnsIn, KeyName :: HNil, ColumnsValues]): KeyMapperData[T, Key, SortKey, KeyValue :: HNil] = {
  //      def makeTable(tableName: String) = {
  //        val columns = builder.mapper.columns
  //        val params = allValues.parameters(columns)
  //        val toRS = colsToRS.toRS(columns).map(_.map(builder.mapper.fromColumns))
  //        DynamoDBTable[T, KeyValue, SortKey](tableName, NamedColumn(keyName.value.name, selectedKeys(columns).head.atom), None,
  //          t => (keyValue(valuesToRecord(builder.mapper.toColumns(t))), None), t => params(builder.mapper.toColumns(t)), allColumnNames(columns), toRS)
  //      }
  //      def makeQueries(dbTable: DynamoDBTable[T, KeyValue, SortKey]): RelationOperations.Aux[T, KeyValue :: HNil] = new AbstractRelationOperations[T](dbTable) {
  //        type Key = KeyValue :: HNil
  //
  //        def keyColumns: List[ColumnName] = dbTable.keyColumns
  //
  //        def keyParameters(key: Key): Iterable[QueryParam] = dbTable.keyParameters((key.head, None, None))
  //
  //        def sortColumns: List[ColumnName] = List.empty
  //      }
  //      KeyMapperData(makeTable, makeQueries)
  //    }
  //  }
  //
  //  implicit def sortKeyMapper[T, ColumnsIn <: HList, Keys <: HList, ColumnsValues <: HList,
  //  KeyName <: Symbol, KeysOnly <: HList, KeyValue, SortKeyName <: Symbol, SortKeyValue,
  //  KeyField, SortKeyField, AllColumnKeys <: HList, CVRecord <: HList,
  //  AfterRemove <: HList]
  //  (implicit
  //   keysOnly: SelectAllRecord.Aux[ColumnsIn, Keys, KeysOnly],
  //   evPK: KeyField =:= FieldType[KeyName, ColumnMapping[T, KeyValue]],
  //   removePK: Remove.Aux[KeysOnly, KeyField, (KeyField, AfterRemove)],
  //   valsOfSK: ColumnValuesType.Aux[AfterRemove, SortKeyValue :: HNil],
  //   sortKeyName: Keys.Aux[AfterRemove, SortKeyName :: HNil],
  //   evSK: AfterRemove =:= (FieldType[SortKeyName, ColumnMapping[T, SortKeyValue]] :: HNil),
  //   allColumnKeys: Keys.Aux[ColumnsIn, AllColumnKeys],
  //   allValues: ToQueryParameters.Aux[ColumnsIn, ColumnsValues],
  //   valuesToRecord: ZipWithKeys.Aux[AllColumnKeys, ColumnsValues, CVRecord],
  //   selectPK: Selector.Aux[CVRecord, KeyName, KeyValue],
  //   selectSK: Selector.Aux[CVRecord, SortKeyName, SortKeyValue],
  //   allColumnNames: ColumnNames[ColumnsIn],
  //   colsToRS: ColumnsAsRS.Aux[ColumnsIn, ColumnsValues])
  //  = new KeyMapper[T, ColumnsIn, Keys, ColumnsValues, KeyName :: HNil, KeyValue :: HNil] {
  //    type ColumnsOut = SortKeyName :: SortKeyValue :: HNil
  //    type SortKey = SortKeyValue
  //    type Key = KeyValue
  //
  //    def table(builder: RelationBuilder.Aux[T, ColumnsIn, Keys, ColumnsValues]): KeyMapperData[T, KeyValue, SortKeyValue, KeyValue :: HNil] = {
  //      def makeTable(tableName: String) = {
  //        val columns = builder.mapper.columns
  //        val params = allValues.parameters(columns)
  //        val (pkCol, skCol) = removePK(keysOnly(columns)) match {
  //          case (_pkCol, _skCol) => (evPK(_pkCol), evSK(_skCol).head)
  //        }
  //        val toRS = colsToRS.toRS(columns).map(_.map(builder.mapper.fromColumns))
  //        def toKeys(t: T) = {
  //          val cvRecord = valuesToRecord(builder.mapper.toColumns(t))
  //          (selectPK(cvRecord), Some(selectSK(cvRecord)))
  //        }
  //        DynamoDBTable[T, KeyValue, SortKeyValue](tableName, NamedColumn[KeyValue](pkCol.name.name, pkCol.atom),
  //          Some(NamedColumn[SortKeyValue](skCol.name.name, skCol.atom)),
  //          toKeys, t => params(builder.mapper.toColumns(t)), allColumnNames(columns), toRS)
  //      }
  //      def makeQueries(dbTable: DynamoDBTable[T, KeyValue, SortKey]): RelationOperations.Aux[T, KeyValue :: HNil] = new AbstractRelationOperations[T](dbTable) {
  //        type Key = KeyValue :: HNil
  //
  //        def keyColumns: List[ColumnName] = dbTable.keyColumns ++ dbTable.sortColumns
  //
  //        def keyParameters(key: Key): Iterable[QueryParam] = dbTable.keyParameters((key.head, None, None))
  //
  //        def sortColumns: List[ColumnName] = List.empty
  //      }
  //      KeyMapperData(makeTable, makeQueries)
  //
  //    }
  //
  //  }
  //
  //  implicit def dynamoMapping[T, Columns <: HList, Keys <: HList, ColumnsValues <: HList, KeysToQuery <: HList,
  //  Selected <: HList, SelectedTypes <: HList, ColumnsOut <: HList, KeyOut, SortKeyOut]
  //  (implicit
  //   selectAll: SelectAll.Aux[Columns, KeysToQuery, Selected],
  //   selectedTypes: ColumnValuesType.Aux[Selected, SelectedTypes],
  //   keyMapper: KeyMapper[T, Columns, Keys, ColumnsValues, KeysToQuery, SelectedTypes])
  //  : PhysicalMapping.Aux[T, Columns, Keys, ColumnsValues, KeysToQuery, SelectedTypes]
  //  = new PhysicalMapping[T, Columns, Keys, ColumnsValues, KeysToQuery] {
  //    type KeyValues = SelectedTypes
  //
  //    def apply(t: RelationBuilder.Aux[T, Columns, Keys, ColumnsValues]): DDL[RelationOperations.Aux[T, SelectedTypes]]
  //    = State { (s: PhysicalTables) =>
  //      val tableMaker = keyMapper.table(t)
  //      val dTable = tableMaker.makeTable(t.baseName)
  //      val queries = tableMaker.makeQuery(dTable)
  //      val jb = t: RelationBuilder[T]
  //      val newList = (dTable: WriteableRelation[T]) :: s.map.get(jb).getOrElse(List.empty)
  //      (s.copy(set = s.set + dTable, map = s.map +(jb, newList)), queries)
  //    }
  //  }
  //
  //

  object DynamoDBPhysicalRelation {
    def apply[CR0 <: HList, CVL0 <: HList, S, PKV, SKV, SK, CL <: HList, VZL <: HList]
    (colMapper: ColumnMapper.Aux[S, CR0, CVL0], pkCol: ColumnMapping[S, PKV],
     skColL: SK, toKeys: CVL0 => PKV :: SKV :: HNil)
    (implicit
     cv: ColumnValuesType.Aux[SK, SKV],
     allVals: PhysicalValues[CVL0, CR0],
     pkVals: PhysicalValues[PKV, ColumnMapping[S, PKV]],
     skVals: PhysicalValues[SKV, SK],
     materializeAll: MaterializeFromColumns.Aux[CR0, CVL0]
    ): PhysRelation.Aux[S, (CR0, CVL0), PKV, cv.Out]
    = new DynamoPhysRelation[S] {
      type CR = CR0
      type CVL = CVL0
      type PartitionKey = PKV
      type SortKey = SKV

      def mapper = colMapper
      val columns = colMapper.columns
      val materializer = materializeAll(columns)

      def asAttrMap(l: List[PhysicalValue]) = l.map(physical2Attribute).toMap.asJava

      def physical2Attribute(pc: PhysicalValue) = pc.name -> pc.withCol((v, c) => c.to(v))

      def createMaterializer(m: java.util.Map[String, AttributeValue]): ColumnMaterialzer = new ColumnMaterialzer {
        def apply[A](name: String, atom: ColumnAtom[A]): Option[A] = {
          Option(m.get(name)).map(av => atom.from(atom.physicalColumn.from(av)))
        }
      }

      def createReadQueries(tableName: String): ReadQueries = new ReadQueries {
        def selectOne[A](projection: DynamoProjection[A], where: DynamoWhere, asc: Boolean): Effect[Option[A]] = Reader { s =>
          val m = where match {
            case AttributeMatch(matches) => Option(s.client.getItem(tableName, matches.toMap.asJava).getItem).map(createMaterializer)
          }
          m.flatMap(projection.materialize)
        }

        def selectMany[A](projection: DynamoProjection[A], where: DynamoWhere, asc: Boolean): Effect[List[A]] = Reader { s =>
          val qr = new QueryRequest().withTableName(tableName)
          val qr2 = where match {
            case AttributeMatch(matches) =>
              val exprs = matches.zipWithIndex map {
                case ((n, av), ind) => (n, s"#F$ind", av, s":V$ind")
              }
              val expression = exprs.map { case (_, an, _, vn) => s"$an = $vn" } mkString " AND "
              val names = exprs.map { case (n, nm, _, _) => (nm, n)}.toMap.asJava
              val values = exprs.map { case (_, _, av, vn) => (vn, av) }.toMap.asJava
              qr.withKeyConditionExpression(expression)
                .withExpressionAttributeNames(names)
                .withExpressionAttributeValues(values)
          }
          s.client.query(qr2).getItems.asScala.flatMap(v => projection.materialize(createMaterializer(v))).toList
        }

        def whereFullKey(pk: PartitionKey :: SortKey :: HNil): DynamoWhere
        = AttributeMatch((pkVals(pk.head, pkCol) ++ skVals(pk.tail.head, skColL)).map(physical2Attribute))

        def wherePK(pk: PartitionKey): DynamoWhere = AttributeMatch(pkVals(pk, pkCol).map(physical2Attribute) )

        def whereRange(pk: PKV, lower: SortKey, upper: SortKey): DynamoWhere = ???
      }

      def createWriteQueries(tableName: String): WriteQueries[S] = new WriteQueries[S] {
        def delete(t: S): Effect[Unit] = Reader { s =>
          val keyHList = toKeys(mapper.toColumns(t))
          val (pk, sk) = (keyHList.head, keyHList(1))
          s.client.deleteItem(tableName, asAttrMap(pkVals(pk, pkCol) ++ skVals(sk, skColL)))
        }

        def insert(t: S): Effect[Unit] = Reader { s =>
          s.client.putItem(tableName, asAttrMap(allVals(mapper.toColumns(t), columns)))
        }

        def update(existing: S, newValue: S): Effect[Boolean] = Reader { s => false }
      }

      def createDDL(tableName: String): CreateTableRequest = ???


      def convertKey(other: PhysRelation[S]): (other.FullKey) => FullKey = ???
    }
  }

//  case class DynamoDBTable[T, Key, SortKey0]
//  (tableName: String, keyColumn: NamedColumn[Key],
//   sortKey: Option[NamedColumn[SortKey0]],
//   allColumns: List[NamedColumn[_]],
//   extract: T => (Eval[(Key, Option[SortKey0])], Eval[List[_]]), materialize: List[_] => T) extends WriteQueries[T] with RelationQueries[T] {
//
//    type PartitionKey = Key
//    type SortKey = SortKey0
//
//    def createTable: CreateTableRequest = {
//      val sortDef = (List(keyColumn) ++ sortKey).map(c => new AttributeDefinition(c.name, c.column.physicalColumn.attributeType))
//      val keyDef = new KeySchemaElement(keyColumn.name, KeyType.HASH)
//      val sortKeyDef = sortKey.map(c => new KeySchemaElement(c.name, KeyType.RANGE))
//      new CreateTableRequest(sortDef.asJava, tableName, (List(keyDef) ++ sortKeyDef).asJava, new ProvisionedThroughput(1L, 1L))
//    }
//
//    def materializeValue(attrMap: scala.collection.mutable.Map[String, AttributeValue]): T = {
//      materialize(allColumns.map { nc =>
//        val atom = nc.column
//        atom.from(atom.physicalColumn.from(attrMap.getOrElse(nc.name, throw new RuntimeException("Failed to get field"))))
//      })
//    }
//
//    def attrValuePair[A](col: (A, NamedColumn[A])) = col._2.name -> attrValue(col._1, col._2.column)
//
//    def attrValue[A](a: A, atom: ColumnAtom[A]): AttributeValue =
//      atom.withColumn(a, (t, c) => c.to(t))
//
//    def keyAttributeValues(t: T): Map[String, AttributeValue] = {
//      val (pkv, skO) = extract(t)._1.value
//      val justPK = Map(keyColumn.name -> attrValue(pkv, keyColumn.column))
//      sortKey.flatMap(sk => skO.map(skv => justPK + (sk.name -> attrValue(skv, sk.column)))).getOrElse(justPK)
//    }
//
//    def delete(t: T): Effect[Unit] = Reader { db => db.client.deleteItem(tableName, keyAttributeValues(t).asJava); () }
//
//    def update(existing: T, newValue: T): Effect[Boolean] = ???
//
//    def insert(t: T): Effect[Unit] = Reader { db =>
//      val attrValues = extract(t)._2.value.zip(allColumns).map(a => attrValuePair((a._1.asInstanceOf[Any], a._2.asInstanceOf[NamedColumn[Any]])))
//      db.client.putItem(tableName, attrValues.toMap.asJava)
//    }
//
//    def queryWithFullKey(k: Key, sortKey: SortKey0): Effect[Option[T]] = Reader { db => db.client.getItem() }
//
//    def queryRange(k: Key, lower: SortKey0, upper: SortKey0, ascending: Boolean): Effect[List[T]] = ???
//
//    def queryWithPartitionKey(k: Key): Effect[List[T]] = ???
//  }

  //    val keyColumns = List(ColumnName(keyColumn.name))
  //    val sortColumns = sortKey.map(c => ColumnName(c.name)).toList
  //
  //    def allParameters(value: T) = allParamValues(value)
  //
  //    def toKey(t: T) = {
  //      val (k, skO) = _toKey(t)
  //      (k, skO, None)
  //    }
  //
  //    def keyParameters(key: (Key, Option[SortKey], Option[SortKey])): Iterable[QueryParam] = {
  //      Iterable(keyColumn.column.queryParameter(key._1)) ++
  //        key._2.flatMap(skv => sortKey.map(_.column.queryParameter(skv))) ++
  //        key._3.flatMap(skv => sortKey.map(_.column.queryParameter(skv)))
  //    }
  //
  //    def keyParametersFromValue(value: T): Iterable[QueryParam] = keyParameters(toKey(value))
  //
  //    def diff(value1: T, value2: T): Xor[Iterable[QueryParam], (List[ColumnDifference], Iterable[QueryParam])] = {
  //      val val1PK = toKey(value1)
  //      val val2PK = toKey(value2)
  //      if (val1PK == val2PK) Xor.right {
  //        val all1Params = allParameters(value1)
  //        val all2Params = allParameters(value2)
  //        (allColumns.zip(all1Params).zip(all2Params).collect {
  //          case ((name, orig), newValue) if orig.v != newValue.v => ColumnDifference(name, newValue, orig)
  //        }, keyParameters(val2PK))
  //      } else {
  //        Xor.Left(keyParameters(val1PK))
  //      }
  //    }

}
