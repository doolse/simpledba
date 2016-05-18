package io.doolse.simpledba.dynamodb

import cats.{Eval, Id, Monad}
import cats.data.{Reader, State, Xor}
import com.amazonaws.services.dynamodbv2.model._
import io.doolse.simpledba.dynamodb.DynamoDBRelationIO.{Effect, ResultOps}
import io.doolse.simpledba.{ColumnName, RelationMapper}
import shapeless._
import shapeless.labelled.FieldType
import shapeless.ops.hlist.{IsHCons, ToList, ZipWithKeys}
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
  type PhysRelationT[T, Meta] = PhysRelation[T, Meta]

  implicit def noSortKeyMapper[T, CR <: HList, KL <: HList, CVL <: HList,
  PKK, PKL <: HList, PKC, PKV
  ]
  (implicit
   pkk: IsHCons.Aux[PKL, PKK, HNil],
   ev: IsHCons.Aux[KL, PKK, HNil],
   pkColumn: Selector.Aux[CR, PKK, PKC],
   valType: ColumnValuesType.Aux[PKC, PKV]
  )
  : KeyMapper.Aux[T, CR, KL, CVL, PKL, Nothing, PKV, HNil] = new KeyMapper[T, CR, KL, CVL, PKL] {
    type Meta = Nothing
    type PartitionKey = PKV
    type SortKey = HNil

    def apply(t: RelationBuilder.Aux[T, CR, KL, CVL]): PhysRelation.Aux[T, Nothing, PKV, HNil] = ???
  }

  implicit def dynamoDBRemainingSortKeyMapper[T, CR <: HList, KL <: HList, CVL <: HList,
  PKK, SKK, KeysOnlyR <: HList,
  PKL <: HList, KLT <: HList, RemainingSK <: HList, KVL <: HList, PKV, SKV, KVLT <: HList
  ]
  (implicit
   pkk: IsHCons.Aux[PKL, PKK, HNil],
   removePK: hlist.Remove.Aux[KL, PKK, (PKK, RemainingSK)],
   skk: IsHCons.Aux[RemainingSK, SKK, HNil],
   keysOnly: SelectAllRecord.Aux[CR, PKK :: SKK :: HNil, KeysOnlyR],
   ev: ColumnValuesType.Aux[KeysOnlyR, KVL],
   pkv: IsHCons.Aux[KVL, PKV, KVLT],
   skv: IsHCons.Aux[KVLT, SKV, HNil]
  )

  : KeyMapper.Aux[T, CR, KL, CVL, PKL, Nothing, PKV, SKV] = ???

  implicit def dynamoDBAutoSortKeyMapper[T, CR <: HList, KL <: HList, CVL <: HList,
  PKK, SKK, KeysOnlyR <: HList,
  PKL <: HList, PKLT <: HList
  ]
    (implicit
     pkk: IsHCons.Aux[PKL, PKK, PKLT],
     skk: IsHCons.Aux[PKLT, SKK, HNil],
     keysOnly: SelectAllRecord.Aux[CR, PKK :: SKK :: HNil, KeysOnlyR])

  : KeyMapper.Aux[T, CR, KL, CVL, PKL, Nothing, Nothing, Nothing] = ???

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
    def apply[CR <: HList, S, PKV, SKL <: HList](pkCol: ColumnMapping[S, PKV], skl: SKL, cv: ColumnValuesType[SKL])(implicit toList: ToList[]): PhysRelation[T, CR] = new PhysRelation[T, CR] {
      type PartitionKey = PKV
      type SortKey = cv.Out

      def isFullKeyCompatibleWith[OPK, OSK](other: PhysRelation.Aux[T, _, OPK, OSK]): (other.FullKey) => FullKey = ???

      def createReadQueries(tableName: String): WriteQueries[T] = ???

      def createWriteQueries(tableName: String): ReadQueries = ???

      def createDDL(tableName: String): CreateTableRequest = ???
    }
  }

  case class DynamoDBTable[T, Key, SortKey0]
  (tableName: String, keyColumn: NamedColumn[Key],
   sortKey: Option[NamedColumn[SortKey0]],
   allColumns: List[NamedColumn[_]],
   extract: T => (Eval[(Key, Option[SortKey0])], Eval[List[_]]), materialize: List[_] => T) extends WriteQueries[T] with RelationQueries[T] {

  type PartitionKey = Key
  type SortKey = SortKey0

  def createTable: CreateTableRequest = {
    val sortDef = (List(keyColumn) ++ sortKey).map(c => new AttributeDefinition(c.name, c.column.physicalColumn.attributeType))
    val keyDef = new KeySchemaElement(keyColumn.name, KeyType.HASH)
    val sortKeyDef = sortKey.map(c => new KeySchemaElement(c.name, KeyType.RANGE))
    new CreateTableRequest(sortDef.asJava, tableName, (List(keyDef) ++ sortKeyDef).asJava, new ProvisionedThroughput(1L, 1L))
  }

  def materializeValue(attrMap: scala.collection.mutable.Map[String, AttributeValue]): T = {
    materialize(allColumns.map { nc =>
      val atom = nc.column
      atom.from(atom.physicalColumn.from(attrMap.getOrElse(nc.name, throw new RuntimeException("Failed to get field"))))
    } )
  }

  def attrValuePair[A](col: (A, NamedColumn[A])) = col._2.name -> attrValue(col._1, col._2.column)

  def attrValue[A](a: A, atom: ColumnAtom[A]): AttributeValue =
    atom.withColumn(a, (t, c) => c.to(t))

  def keyAttributeValues(t: T) : Map[String, AttributeValue] = {
    val (pkv, skO) = extract(t)._1.value
    val justPK = Map(keyColumn.name -> attrValue(pkv, keyColumn.column))
    sortKey.flatMap(sk => skO.map(skv => justPK + (sk.name -> attrValue(skv, sk.column)))).getOrElse(justPK)
  }

  def delete(t: T): Effect[Unit] = Reader { db => db.client.deleteItem(tableName, keyAttributeValues(t).asJava); () }

  def update(existing: T, newValue: T): Effect[Boolean] = ???

  def insert(t: T): Effect[Unit] = Reader { db =>
    val attrValues = extract(t)._2.value.zip(allColumns).map(a => attrValuePair((a._1.asInstanceOf[Any], a._2.asInstanceOf[NamedColumn[Any]])))
    db.client.putItem(tableName, attrValues.toMap.asJava)
  }

  def queryWithFullKey(k: Key, sortKey: SortKey0): Effect[Option[T]] = Reader { db => db.client.getItem() }

  def queryRange(k: Key, lower: SortKey0, upper: SortKey0, ascending: Boolean): Effect[List[T]] = ???

  def queryWithPartitionKey(k: Key): Effect[List[T]] = ???
}

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
