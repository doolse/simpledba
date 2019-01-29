package io.doolse.simpledba.jdbc

import io.doolse.simpledba.{ColumnBuilder, ColumnSubset, Iso}
import shapeless.{HList, HNil, LabelledGeneric}

object TableMapper {

  def apply[T](implicit jdbcConfig: JDBCConfig) = new RelationBuilder[T, jdbcConfig.C](jdbcConfig)

  class RelationBuilder[T, C[_] <: JDBCColumn](config: JDBCConfig)
  {
    def embedded[GR <: HList, R <: HList]
    (implicit
     gen: LabelledGeneric.Aux[T, GR],
     columns: ColumnBuilder.Aux[C, GR, R]):
     ColumnBuilder.Aux[C, T, R] = new ColumnBuilder[C, T] {
      type Repr = R
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
