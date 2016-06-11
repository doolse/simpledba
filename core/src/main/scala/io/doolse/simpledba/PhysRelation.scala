package io.doolse.simpledba

import shapeless.{::, HList, HNil}

/**
  * Created by jolz on 8/06/16.
  */
trait KeyMapper[T, CR <: HList, KL <: HList, CVL <: HList, Query] {
  type PartitionKey
  type SortKey
  type PartitionKeyNames
  type SortKeyNames
  type Out

  def keysMapped(cm: ColumnMapper[T, CR, CVL])(name: String): Out
}

object KeyMapper {
  trait Impl[T, CR <: HList, KL <: HList, CVL <: HList, Query, PKN0, PartitionKey0, SKN0, SortKey0, Out0] extends KeyMapper[T, CR, KL, CVL, Query] {
    type PartitionKey = PartitionKey0
    type SortKey = SortKey0
    type PartitionKeyNames = PKN0
    type SortKeyNames = SKN0
    type Out = Out0
  }
}

trait PhysRelation[F[_], DDLStatement, T] {
  type SortKey
  type FullKey = PartitionKey :: SortKey :: HNil
  type Projection[A]
  type Where
  type PartitionKey

  trait ReadQueries {
    def selectOne[A](projection: Projection[A], where: Where): F[Option[A]]

    def selectMany[A](projection: Projection[A], where: Where, asc: Option[Boolean]): F[List[A]]
  }

  def selectAll: Projection[T]

  def createReadQueries: ReadQueries

  def createWriteQueries: WriteQueries[F, T]

  def createDDL: DDLStatement

  def whereFullKey(pk: FullKey): Where

  def whereRange(pk: PartitionKey, lower: RangeValue[SortKey], upper: RangeValue[SortKey]): Where
}

object PhysRelation {
  type Aux[F[_], DDLStatement, T, PKV0, SKV0] = PhysRelation[F, DDLStatement, T] {
    type PartitionKey = PKV0
    type SortKey = SKV0
  }
}
