package io.doolse.simpledba2.jdbc

import io.doolse.simpledba2.{ColumnBuilder, KeySubset}
import shapeless.{::, HList, HNil, LabelledGeneric, Witness}

object TableMapper {

  def apply[T](implicit jdbcConfig: JDBCSQLConfig) = new RelationBuilder[T, jdbcConfig.C](jdbcConfig)

  class RelationBuilder[T, C[_] <: JDBCColumn](config: JDBCSQLConfig)
  {
    def embedded[GRepr <: HList, Repr0 <: HList]
    (implicit
     gen: LabelledGeneric.Aux[T, GRepr],
     columns: ColumnBuilder.Aux[C, GRepr, Repr0]):
     ColumnBuilder.Aux[C, T, Repr0] = new ColumnBuilder[C, T] {
      type Repr = Repr0
      def apply() = columns().isomap(gen.from, gen.to)
    }

    def table[GRepr <: HList, Repr <: HList, KName <: Symbol, Key <: HList]
    (tableName: String, key: Witness.Aux[KName])(
      implicit
      gen: LabelledGeneric.Aux[T, GRepr],
      allRelation: ColumnBuilder.Aux[C, GRepr, Repr],
      ss: KeySubset.Aux[Repr, KName :: HNil, Key])
    : JDBCTable.Aux[T, Repr, Key] = {
      val all = allRelation.apply().isomap(gen.from, gen.to)
      val keys = all.subset[KName :: HNil]
      JDBCTable(tableName, config, all, keys)
    }

  }
}
