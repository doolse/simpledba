package io.doolse.simpledba.dynamodb

import io.doolse.simpledba.ColumnBuilder
import shapeless.ops.record.Selector
import shapeless.{HList, LabelledGeneric, Witness}

class DynamoDBMapper {

  def mapped[T] = new RelationBuilder[T]

  class RelationBuilder[T] {
    def table[R <: HList, GR <: HList, PKS <: Symbol, PK](name: String, pk: Witness.Aux[PKS])
                        (implicit lgen: LabelledGeneric.Aux[T, R],
                         columns: ColumnBuilder.Aux[DynamoDBColumn, R, GR],
                         pkCol: Selector.Aux[R, PKS, PK],
                            isPK:DynamoDBPKColumn[PK]
                        ) = {
      val cols = columns.apply()
      val pkName = pk.value.name
      val pkCol = cols.columns.find(_._1 == pkName).get
      DynamoDBTable[T, GR, R, PK, Nothing](name, pkCol.asInstanceOf[(String, DynamoDBColumn[PK])], None, columns.apply())
    }
  }
}
