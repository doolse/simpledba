package io.doolse.simpledba.dynamodb

import cats.Id
import io.doolse.simpledba.{ColumnName, Mapper2}
import io.doolse.simpledba.dynamodb.DynamoDBRelationIO.ResultOps
import shapeless.labelled.FieldType
import shapeless.ops.hlist.Mapper
import shapeless.ops.record.SelectAllRecord
import shapeless.{DepFn0, HList, HNil}

/**
  * Created by jolz on 12/05/16.
  */
class DynamoDBMapper2 extends Mapper2(DynamoDBRelationIO()) {
  type DDL[A] = Id[A]


  implicit def allPKKeyRelation[T, Columns <: HList, ColumnsValues <: HList,
  Keys <: HList, Selected <: HList, KeyNames <: HList]
  (implicit allKeyColumns: SelectAllRecord.Aux[Columns, Keys, Selected],
   keyNames: ColumnNames[Selected],
   allColumnNames: ColumnNames[Columns],
   keyTypes: ColumnValuesType[Selected]) = new PhysicalMapping[T, Columns, ColumnsValues, Keys, Keys] {
    type KeyValues = keyTypes.Out

    def apply(t: RelationBuilder[T, Columns, ColumnsValues, Keys]): Id[RelationOperations[T, keyTypes.Out]] = new RelationOperations[T, KeyValues] {
      def tableName: String = t.baseName

      def keyColumns: List[ColumnName] = keyNames(allKeyColumns(t.mapper.columns))

      def parameters(key: KeyValues): Iterable[QueryParam] = ???

      def allColumns: List[ColumnName] = allColumnNames(t.mapper.columns)

      def fromResultSet: ResultOps[Option[T]] = ???
    }
  }
}
