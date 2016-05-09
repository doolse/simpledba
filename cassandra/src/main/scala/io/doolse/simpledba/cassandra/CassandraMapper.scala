package io.doolse.simpledba.cassandra

import com.datastax.driver.core.TypeCodec
import io.doolse.simpledba.RelationMapper
import shapeless.HList
import fs2.interop.cats._

/**
  * Created by jolz on 8/05/16.
  */
class CassandraMapper extends RelationMapper[CassandraRelationIO.Effect, CassandraRelationIO.ResultSetT] {
  type CT[A] = CassandraColumn[A]
  type DDL = Nothing
  val connection = CassandraRelationIO()
  val resultSet = connection.rsOps

  implicit val longCol : CassandraColumn[Long] = CassandraCodecColumn(TypeCodec.bigint(), Long2long, _.asInstanceOf[AnyRef])
  implicit val boolCol : CassandraColumn[Boolean] = CassandraCodecColumn(TypeCodec.cboolean(), Boolean2boolean, _.asInstanceOf[AnyRef])
  implicit val stringCol  = CassandraCodecColumn.direct[String](TypeCodec.varchar())

  def genDDL(physicalTable: PhysicalTable[_, _, _]): Nothing = ???

  implicit def keyMapper[T, TR <: HList, V <: HList, K <: HList, SK <: HList, FKV <: HList]
  (implicit fullKeyQ: KeyQueryBuilder.Aux[TR, K, FKV])
  : KeySelector.Aux[T, TR, V, (K, SK), TR, V, FKV, FKV]
  = new KeySelector[T, TR, V, (K, SK)] {
    type Out = (TR, KeyQuery[FKV], KeyQuery[FKV], V => V, V => V)

    def apply(columns: TR): Out = {
      (columns, fullKeyQ(columns), fullKeyQ(columns), identity, identity)
    }
  }

}
