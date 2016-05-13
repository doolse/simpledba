package io.doolse.simpledba.dynamodb

import cats.{Id, Monad}
import cats.data.{State, Xor}
import io.doolse.simpledba.dynamodb.DynamoDBRelationIO.ResultOps
import io.doolse.simpledba.{ColumnName, Mapper2}
import shapeless._
import shapeless.ops.hlist.ZipWithKeys
import shapeless.ops.record.{Keys, SelectAll, Values}

/**
  * Created by jolz on 12/05/16.
  */


class DynamoDBMapper2 extends Mapper2[DynamoDBRelationIO.Effect, DynamoDBRelationIO.ResultOps, DynamoDBColumn](DynamoDBRelationIO()) {

  type DDL[A] = State[PhysicalTables, A]

  def DDLMonad = Monad[DDL]

  case class PhysicalTables(map: Map[String, List[RelationOperations[_, _]]])

  implicit def allPKKeyRelation[T, Columns <: HList, ColumnsValues <: HList,
  Keys <: HList, Selected <: HList, SelectedTypes <: HList, AllColumns <: HList, AllKeys <: HList, CVRecord <: HList]
  (implicit
   columnsOnly: Values.Aux[Columns, AllColumns],
   allKeys: Keys.Aux[Columns, AllKeys],
   colValsRecord: ZipWithKeys.Aux[AllKeys, ColumnsValues, CVRecord],
   allKeyColumns: SelectAll.Aux[Columns, Keys, Selected],
   keyParams: ToQueryParameters.Aux[Selected, SelectedTypes],
   allParams: ToQueryParameters.Aux[AllColumns, ColumnsValues],
   keysFromVals: SelectAll.Aux[CVRecord, Keys, SelectedTypes],
   allColumnNames: ColumnNames[AllColumns],
   keyNames: ColumnNames[Selected],
   asRSOps: ColumnsAsRS.Aux[AllColumns, ColumnsValues]
  ) = new PhysicalMapping[T, Columns, ColumnsValues, Keys, Keys] {
    type KeyValues = SelectedTypes

    def apply(t: RelationBuilder[T, Columns, ColumnsValues, Keys]): DDL[RelationOperations[T, SelectedTypes]]
    = State { (s: PhysicalTables) =>
      val newTable = new RelationOperations[T, SelectedTypes] {
        val mapper = t.mapper
        val columns = mapper.columns
        val allColumnsValues = columnsOnly(columns)
        val selectedColumns = allKeyColumns(columns)
        val paramFunc = keyParams.parameters(selectedColumns)
        val allParamsFunc = allParams.parameters(allColumnsValues)

        def tableName: String = t.baseName

        def keyColumns: List[ColumnName] = keyNames(selectedColumns)

        def keyParameters(key: SelectedTypes): Iterable[QueryParam] = paramFunc(key)

        def allColumns: List[ColumnName] = allColumnNames(allColumnsValues)

        def fromResultSet: ResultOps[Option[T]] = asRSOps.toRS(allColumnsValues).map(_.map(mapper.fromColumns))

        def keyFromValue(value: T): SelectedTypes = keysFromVals(colValsRecord(mapper.toColumns(value)))

        def allParameters(value: T): Iterable[QueryParam] = allParamsFunc(mapper.toColumns(value))

        def diff(value1: T, value2: T): Xor[Iterable[QueryParam], List[ColumnDifference]] = {
          val value1Cols = mapper.toColumns(value1)
          val value2Cols = mapper.toColumns(value2)
          val val1PK = keysFromVals(colValsRecord(value1Cols))
          val val2PK = keysFromVals(colValsRecord(value2Cols))
          if (val1PK == val2PK) Xor.right {
            val all1Params = allParamsFunc(value1Cols)
            val all2Params = allParamsFunc(value2Cols)
            allColumns.zip(all1Params).zip(all2Params).collect {
              case ((name, orig), newValue) if orig.v != newValue.v => ColumnDifference(name, newValue, orig)
            }
          } else {
            Xor.Left(keyParameters(val1PK))
          }

        }
      }
      (s.copy(map = s.map.updated(t.baseName, s.map.getOrElse(t.baseName, List.empty) :+ newTable)), newTable)
    }
  }

  def getRelationsForBuilder[T](forBuilder: RelationBuilder[T, _, _, _]): State[PhysicalTables, List[RelationOperations[T, _]]] = State.inspect {
    s => s.map.getOrElse(forBuilder.baseName, List.empty).asInstanceOf[List[RelationOperations[T, _]]]
  }

  def build[A](ddl: State[PhysicalTables, A]): A = ddl.runA(PhysicalTables(Map.empty)).value
}
