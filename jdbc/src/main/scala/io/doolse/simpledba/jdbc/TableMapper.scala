package io.doolse.simpledba.jdbc

import io.doolse.simpledba.{ColumnBuilder, ColumnSubset, Iso}
import shapeless.{HList, HNil, LabelledGeneric}

object TableMapper {

  def apply[T](implicit jdbcConfig: JDBCConfig) = new RelationBuilder[T, jdbcConfig.C](jdbcConfig)

  class RelationBuilder[T, C[_] <: JDBCColumn](config: JDBCConfig)
  {
    def embedded[GRepr <: HList, Repr0 <: HList]
    (implicit
     gen: LabelledGeneric.Aux[T, GRepr],
     columns: ColumnBuilder.Aux[C, GRepr, Repr0]):
     ColumnBuilder.Aux[C, T, Repr0] = new ColumnBuilder[C, T] {
      type Repr = Repr0
      def apply() = columns().compose(Iso(gen.to, gen.from))
    }

    def table[GR <: HList, R <: HList](tableName: String)(
      implicit
      gen: LabelledGeneric.Aux[T, GR],
      allRelation: ColumnBuilder.Aux[C, GR, R]
    ): JDBCRelation[C, T, R] =
      JDBCRelation[C, T, R](tableName, config, allRelation()
        .compose(Iso(gen.to, gen.from)))
  }
}
