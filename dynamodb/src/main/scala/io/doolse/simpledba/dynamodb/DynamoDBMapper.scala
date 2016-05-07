package io.doolse.simpledba.dynamodb

import cats.~>
import com.amazonaws.services.dynamodbv2.model._
import io.doolse.simpledba.RelationIO.Aux
import io.doolse.simpledba._
import io.doolse.simpledba.dynamodb.DynamoDBRelationIO.{Effect, ResultOps}
import shapeless._
import shapeless.labelled.FieldType
import shapeless.ops.hlist.{Mapped, Mapper, NatTRel, ToList, ZipWithIndex, ZipWithKeys}
import shapeless.ops.record.{Fields, Keys, SelectAll, Values}

import scala.collection.JavaConverters._

/**
  * Created by jolz on 5/05/16.
  */

class DynamoDBMapper extends RelationMapper {
  type IO[A] = DynamoDBRelationIO.Effect[A]
  type DDL = CreateTableRequest
  type CT[A] = DynamoDBColumn[A]
  type ResultSetOps[A] = DynamoDBRelationIO.ResultOps[A]

  implicit val longColumn = DynamoDBColumn[Long](_.getN.toLong, l => new AttributeValue().withN(l.toString), ScalarAttributeType.N)
  implicit val boolColumn = DynamoDBColumn[Boolean](_.getBOOL, b => new AttributeValue().withBOOL(b), ScalarAttributeType.S)
  implicit val stringColumn = DynamoDBColumn[String](_.getS, new AttributeValue(_), ScalarAttributeType.S)

  implicit def catom[T](implicit dcol: DynamoDBColumn[T]): ColumnAtom[T] = new ColumnAtom[T] {
    type ColType = T

    def column = dcol
  }

  //  object field2Physical extends Poly1 {
  //    implicit def allFields[S <: Symbol, T] =
  //      at[(S, ColumnAtom[T])](f => PhysicalColumn[T](ColumnMetadata(f._1.name, StandardColumn), f._2))
  //  }
  //
  //  implicit def keyMapper[CR <: HList, V <: HList, K <: HList, SK <: HList, PhysColumns <: HList, Fields <: HList]
  //  (implicit fields: Fields.Aux[CR, Fields],
  //   atoms: Mapper.Aux[field2Physical.type, Fields, PhysColumns],
  //   ks: SelectAll[CR, K]) = new KeySelector[(CR, V, K, SK)] {
  //    type ColumnsRecord = PhysColumns
  //    type Values = V
  //    type FullKeyValue = HNil
  //    type PartialKeyValue = HNil
  //
  //    def columns(columnizer: Columnizer.Aux[_, CR, V]): PhysColumns = {
  //      val mappedColumns = columnizer.columns
  //      fields(mappedColumns).map(field2Physical)
  //    }
  //
  //    def fullKeyColumns: List[ColumnName] = ???
  //
  //    def fullKeyParameters(fkv: HNil): Iterable[relIO.QP[Any]] = ???
  //
  //    def partialKeyColumns: List[ColumnName] = ???
  //
  //    def partialKeyParameters(fkv: HNil): Iterable[relIO.QP[Any]] = ???
  //  }

  implicit def keyMapper[T, TR <: HList, V <: HList, K <: HList, SK <: HList, KC <: HList, KS <: HList, KO <: HList]
  (implicit select: SelectAll.Aux[TR, K, KC], zipWithKeys: ZipWithKeys.Aux[K, KC, KS], keys: Keys.Aux[KS, KO], toList: ToList[KO, Symbol])
  = new KeySelector[Columnizer.Aux[T, TR, V], (K, SK)] {
    type ColumnsRecord = TR

    type PartialKeyValue = HNil
    type FullKeyValue = HNil
    type PartialKeyRecord = HNil
    type FullKeyRecord = HNil

    def transform(col: Columnizer.Aux[T, TR, V]): TR = col.columns

    def fullKeyColumns: List[ColumnName] = toList(keys()).map(s => ColumnName(s.name))

    def partialKeyColumns: List[ColumnName] = fullKeyColumns

    def fullKeyParameters(fkv: HNil): Iterable[relIO.QP[Any]] = ???

    def partialKeyParameters(fkv: HNil): Iterable[relIO.QP[Any]] = ???

  }

  implicit def physicalTable[T, K <: HList, SK <: HList, TR <: HList, V <: HList,
  CR <: HList, FKV <: HList, PKV <: HList, PhysFields <: HList]
  (implicit
   columnizer: Columnizer.Aux[T, TR, V],
   keySelect: KeySelector.Aux[Columnizer.Aux[T, TR, V], (K, SK), CR, FKV, PKV],
   fields: Fields.Aux[CR, PhysFields],
   toList: ToList[PhysFields, (_ <: Symbol, ColumnAtom[_])])
  : PhysicalTableBuilder[T, K, SK] = new PhysicalTableBuilder[T, K, SK] {
    type Out = PhysicalTable[T]

    def apply(t: TableBuilder[T, K, SK]) = new PhysicalTable[T] {
      def name: String = t.name

      def fromResultSet: ResultOps[T] = ???

      def columns: List[PhysicalColumn[_]] = {
        val partitionFields = keySelect.partialKeyColumns.map(_.name).toSet
        val fullFields = keySelect.fullKeyColumns.map(_.name).toSet
        toList(fields(keySelect.transform(columnizer))).map {
          case (Symbol(s), cat) => PhysicalColumn(ColumnMetadata(s,
            if (partitionFields.contains(s)) PartitionKey else if (fullFields.contains(s)) SortKey else StandardColumn), cat)
        }
      }

      def genDDL: CreateTableRequest = {
        val keyList = columns.filter(_.meta.columnType == PartitionKey)
        val attrs = keyList.map(c => new AttributeDefinition(c.meta.name, c.column.column.attributeType))
        new CreateTableRequest(attrs.asJava, t.name, keyList.map(column => new KeySchemaElement(column.meta.name, KeyType.HASH)).asJava, new ProvisionedThroughput(1L, 1L))
      }

      val keySelector: KeySelector[_, _] = keySelect
    }
  }

  //  implicit def physicalTable[T, K <: HList, SK <: HList, TR <: HList, V <: HList, PhysCols <: HList]
  //  (implicit columnizer: Columnizer.Aux[T, TR, V], keySelector: KeySelector[(TR, V, K, SK)], toList: ToList[PhysCols, PhysicalColumn[_]]) = new PhysicalTableBuilder[T, K, SK] {
  //    type Out = PhysicalTable[T]
  //
  //    def apply(t: TableBuilder[T, K, SK]): PhysicalTable[T] = new PhysicalTable[T] {
  //      def columns: List[PhysicalColumn[_]] = keySelector.columns(columnizer).toList
  //
  //      def fromResultSet: ResultOps[T] = ???
  //
  //      def genDDL: CreateTableRequest = {
  //
  //        val keyList = columns.filter(_.meta.columnType == PartitionKey)
  //        val attrs = keyList.map(c => new AttributeDefinition(c.meta.name, c.column.column.attributeType))
  //        new CreateTableRequest(attrs.asJava, t.name, keyList.map(column => new KeySchemaElement(column.meta.name, KeyType.HASH)).asJava, new ProvisionedThroughput(1L, 1L))
  //
  //      }
  //
  //      def name: String = t.name
  //    }
  //  }

  //
  //  implicit def abuilder[T, Keys <: HList, SortKeys <: HList, Repr <: HList, SelValues <: HList, SelFields <: HList, H, ColList <: HList, M]
  //  (implicit lg: LabelledGeneric.Aux[T, Repr],
  //   selAll: SelectAll.Aux[Repr, Keys, SelValues], zipped: ZipWithKeys.Aux[Keys, SelValues, SelFields],
  //   columns: Columnizer.Aux[SelFields, ColList], toList: ToList[ColList, ColumnAtom[_]]) = new DDLBuilder[T, Keys, SortKeys] {
  //    def apply(t: TableBuilder[T, Keys, SortKeys]): DDL = {
  //      val keyList = columns.apply().toList
  //      val attrs = keyList.map(c => new AttributeDefinition(c.name, c.column.attributeType))
  //      new CreateTableRequest(attrs.asJava, t.name, keyList.map(column => new KeySchemaElement(column.name, KeyType.HASH)).asJava, new ProvisionedThroughput(1L, 1L))
  //    }
  //  }
  val relIO = new DynamoDBRelationIO
}
