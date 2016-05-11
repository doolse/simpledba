package io.doolse.simpledba

import cats.{Applicative, Monad}
import cats.sequence._
import cats.syntax.functor._
import cats.std.option._
import shapeless.labelled._
import shapeless._
import shapeless.ops.hlist.{Comapped, Drop, Length, Mapped, Mapper, Prepend, Take, ZipConst}
import shapeless.ops.record.{Keys, SelectAll, Selector, Values}
import shapeless.UnaryTCConstraint._

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by jolz on 10/05/16.
  */
class Mapper2[F[_], RSOps[_] : Monad, PhysCol[_]](val connection: RelationIO.Aux[F, RSOps, PhysCol]) {
  type QueryParam = connection.QP

  trait ColumnAtom[A] {
    def getColumn(cr: ColumnReference): RSOps[Option[A]]

    def queryParameter(a: A): QueryParam
  }

  object ColumnAtom {
    implicit def stdColumn[A](implicit col: PhysCol[A], tag: ClassTag[A]) = new ColumnAtom[A] {
      def getColumn(cr: ColumnReference): RSOps[Option[A]] = connection.rsOps.getColumn(cr, col)

      def queryParameter(a: A): QueryParam = connection.parameter(col, a)

      override def toString = tag.runtimeClass.getName
    }
  }

  case class ColumnMapping[S, A](name: ColumnName, atom: ColumnAtom[A], get: S => A)(implicit tt: TypeTag[S]) {
    override def toString = s"('${name.name}',$atom,$tt)"
  }

  @implicitNotFound("Failed to find mapper for ${A}")
  trait ColumnMapper[A] {
    type Columns <: HList
    type ColumnsValues <: HList

    def columns: Columns

    def fromValues: ColumnsValues => A
  }

  object composeLens extends Poly1 {
    implicit def convertLens[T, T2, K, A](implicit tt: TypeTag[T]) = at[(FieldType[K, ColumnMapping[T2, A]], T => T2)] {
      case (colMapping, lens) => field[K](colMapping.copy[T, A](get = colMapping.get compose lens))
    }
  }

  object ColumnMapper {
    type Aux[A, Columns0 <: HList, ColumnsValues0 <: HList] = ColumnMapper[A] {
      type Columns = Columns0
      type ColumnsValues = ColumnsValues0
    }

    implicit val hnilMapper: ColumnMapper.Aux[HNil, HNil, HNil] = new ColumnMapper[HNil] {
      type Columns = HNil
      type ColumnsValues = HNil

      def columns = HNil

      def fromValues = identity
    }

    implicit def singleColumn[K <: Symbol, V](implicit atom: ColumnAtom[V], key: Witness.Aux[K], tt: TypeTag[FieldType[K, V]]):
    ColumnMapper.Aux[FieldType[K, V], FieldType[K, ColumnMapping[FieldType[K, V], V]] :: HNil, V :: HNil] = new ColumnMapper[FieldType[K, V]] {
      type Columns = FieldType[K, ColumnMapping[FieldType[K, V], V]] :: HNil
      type ColumnsValues = V :: HNil

      def columns = field[K](ColumnMapping[FieldType[K, V], V](ColumnName(key.value.name), atom, fv => fv: V)) :: HNil

      def fromValues = v => field[K](v.head)
    }

    implicit def genericColumn[T, Repr <: HList, Columns0 <: HList, ColumnsValues0 <: HList, ColumnsZ <: HList]
    (implicit lgen: LabelledGeneric.Aux[T, Repr], mapping: ColumnMapper.Aux[Repr, Columns0, ColumnsValues0],
     zipWithLens: ZipConst.Aux[T => Repr, Columns0, ColumnsZ], mapper: Mapper[composeLens.type, ColumnsZ]
    ): Aux[T, mapper.Out, ColumnsValues0]
    = new ColumnMapper[T] {
      type Columns = mapper.Out
      type ColumnsValues = ColumnsValues0

      def columns = mapper(zipWithLens(lgen.to, mapping.columns))

      def fromValues = v => lgen.from(mapping.fromValues(v))
    }

    implicit def multiColumn[K <: Symbol, V, Columns0 <: HList, ColumnsValues0 <: HList, CZ <: HList]
    (implicit mapping: ColumnMapper.Aux[V, Columns0, ColumnsValues0],
     zipWithLens: ZipConst.Aux[FieldType[K, V] => V, Columns0, CZ], mapper: Mapper[composeLens.type, CZ]) = new ColumnMapper[FieldType[K, V]] {
      type Columns = mapper.Out
      type ColumnsValues = ColumnsValues0

      def columns = mapper(zipWithLens(v => v: V, mapping.columns))

      def fromValues = v => field[K](mapping.fromValues(v))
    }

    implicit def hconsMapper[H, T <: HList,
    HC <: HList, HCZ <: HList, HCM <: HList,
    TC <: HList, TCZ <: HList, TCM <: HList,
    HV <: HList, TV <: HList,
    OutV <: HList, LenHV <: Nat]
    (implicit headMapper: ColumnMapper.Aux[H, HC, HV], tailMapper: ColumnMapper.Aux[T, TC, TV], tt: TypeTag[T],
     zipHeadLens: ZipConst.Aux[H :: T => H, HC, HCZ], zipTailLens: ZipConst.Aux[H :: T => T, TC, TCZ],
     mappedHead: Mapper.Aux[composeLens.type, HCZ, HCM], mappedTail: Mapper.Aux[composeLens.type, TCZ, TCM],
     prependC: Prepend[HCM, TCM], prependV: Prepend.Aux[HV, TV, OutV],
     lenH: Length.Aux[HV, LenHV], headVals: Take.Aux[OutV, LenHV, HV], tailVals: Drop.Aux[OutV, LenHV, TV])
    : ColumnMapper.Aux[H :: T, prependC.Out, OutV] = {
      new ColumnMapper[H :: T] {
        type Columns = prependC.Out
        type ColumnsValues = OutV

        def columns = prependC(mappedHead(zipHeadLens(_.head, headMapper.columns)), mappedTail(zipTailLens(_.tail, tailMapper.columns)))

        def fromValues = v => headMapper.fromValues(headVals(v)) :: tailMapper.fromValues(tailVals(v))
      }
    }

  }

  object columnMappingToRS extends Poly1 {
    implicit def mappingToRS[S, A] = at[ColumnMapping[S, A]](cm => cm.atom.getColumn(cm.name))
  }

  case class Relation[T, Repr <: HList, Columns <: HList, Keys <: HList, AllValues <: HList](baseName: String,
                                                                                             mapper: ColumnMapper.Aux[T, Columns, AllValues], keys: Keys) {
    def key(k: Witness)(implicit ev: Selector[Columns, k.T]) = copy[T, Repr, Columns, k.T :: Keys, AllValues](keys = k.value :: keys)

    def compositeKey(k1: Witness, k2: Witness)(implicit ev: SelectAll[Columns, k1.T :: k2.T :: HNil]) =
      copy[T, Repr, Columns, k1.T :: k2.T :: Keys, AllValues](keys = k1.value :: k2.value :: keys)

    private implicit val opApp = Applicative[Option]
    def fromRS[ColumnsOnly <: HList, OutRS <: HList, Sequenced <: HList]
    (implicit values: Values.Aux[Columns, ColumnsOnly],
     traverser: Traverser.Aux[ColumnsOnly, columnMappingToRS.type, RSOps[OutRS]],
     sequenceOps: Sequencer.Aux[OutRS, Option[AllValues]]
    ): RSOps[Option[T]] =
      traverser(values(mapper.columns)).map(rs => sequenceOps(rs).map(mapper.fromValues))

    def queryByPK[Sel <: HList](implicit selectAll: Values[Columns]) = selectAll(mapper.columns)
  }

  class RelationPartial[T] {
    def apply[Repr <: HList, Columns <: HList, ColumnsValues <: HList]
    (name: String)(implicit lgen: LabelledGeneric.Aux[T, Repr],
                   mapper: ColumnMapper.Aux[T, Columns, ColumnsValues]): Relation[T, Repr, Columns, HNil, ColumnsValues] = Relation(name, mapper, HNil)
  }

  def relation[T] = new RelationPartial[T]

}
