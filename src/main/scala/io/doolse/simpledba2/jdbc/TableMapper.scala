package io.doolse.simpledba2.jdbc

import io.doolse.simpledba2.ColumnBuilder
import shapeless.{HList, LabelledGeneric}

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

    def table[GR <: HList, R <: HList](tableName: String)(
      implicit
      gen: LabelledGeneric.Aux[T, GR],
      allRelation: ColumnBuilder.Aux[C, GR, R]
    ): JDBCRelation[C, T, R] = JDBCRelation[C, T, R](tableName, config, allRelation.apply().isomap(gen.from, gen.to))
  }
}
