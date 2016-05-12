package io.doolse.simpledba

import cats.{Applicative, Monad}
import cats.sequence._
import cats.syntax.functor._
import cats.std.option._
import shapeless.labelled._
import shapeless._
import shapeless.ops.hlist.{Comapped, Drop, Length, Mapped, Mapper, Prepend, Take, ToList, ZipConst, ZipWithKeys}
import shapeless.ops.record._
import shapeless.UnaryTCConstraint._

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by jolz on 10/05/16.
  */
abstract class Mapper2[F[_], RSOps[_] : Monad, PhysCol[_]](val connection: RelationIO.Aux[F, RSOps, PhysCol]) {
  type QueryParam = connection.QP
  type DDL[A]

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

  trait ColumnValuesType[L <: HList] {
    type Out <: HList
  }

  object ColumnValuesType {
    implicit val hnilColumnType = new ColumnValuesType[HNil] {
      type Out = HNil
    }

    implicit def hconsColumnType[S, K, V, T <: HList](implicit tailTypes: ColumnValuesType[T])
    = new ColumnValuesType[(FieldType[K, ColumnMapping[S, V]]) :: T] {
      type Out = V :: tailTypes.Out
    }
  }

  @implicitNotFound("Failed to find mapper for ${A}")
  trait ColumnMapper[A] {
    type Columns <: HList
    type ColumnsValues <: HList

    def columns: Columns

    def fromValues: ColumnsValues => A
  }

  trait GenericColumnMapper[T] {
    type Columns <: HList
    type ColumnsValues <: HList
    def apply(): ColumnMapper.Aux[T, Columns, ColumnsValues]
  }

  object GenericColumnMapper {
    def apply[T](implicit genMapper: GenericColumnMapper[T]): ColumnMapper.Aux[T, genMapper.Columns, genMapper.ColumnsValues] = genMapper.apply()

    type Aux[T, Columns0 <: HList, ColumnsValues0 <: HList] = GenericColumnMapper[T] {
    type Columns = Columns0
    type ColumnsValues = ColumnsValues0
    }

    implicit def genericColumn[T, Repr <: HList, Columns0 <: HList, ColumnsValues0 <: HList, ColumnsZ <: HList]
    (implicit lgen: LabelledGeneric.Aux[T, Repr], mapping: ColumnMapper.Aux[Repr, Columns0, ColumnsValues0],
                                         zipWithLens: ZipConst.Aux[T => Repr, Columns0, ColumnsZ], mapper: Mapper[composeLens.type, ColumnsZ]
    ): Aux[T, mapper.Out, ColumnsValues0]
    = new GenericColumnMapper[T] {
      type Columns = mapper.Out
      type ColumnsValues = ColumnsValues0

      def apply() = new ColumnMapper[T] {
        type Columns = mapper.Out
        type ColumnsValues = ColumnsValues0

        def columns = mapper(zipWithLens(lgen.to, mapping.columns))

        def fromValues = v => lgen.from(mapping.fromValues(v))
      }
    }
  }

  trait ColumnNames[Columns <: HList] extends (Columns => List[ColumnName])
  object ColumnNames {
    implicit def columnNames[L <: HList, LM <: HList](implicit mapper: Mapper.Aux[columnNamesFromColumnRecords.type, L, LM], toList: ToList[LM, ColumnName]) = new ColumnNames[L] {
      def apply(columns: L): List[ColumnName] = toList(mapper(columns))
    }
  }

  object columnNamesFromColumnRecords extends Poly1 {
    implicit def mappingToName[K, S, A] = at[FieldType[K, ColumnMapping[S, A]]](_.name)
  }

  object columnMappingToRS extends Poly1 {
    implicit def mappingToRS[S, A] = at[ColumnMapping[S, A]](cm => cm.atom.getColumn(cm.name))
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


  case class SingleQuery[T, KeyValues](query: KeyValues => F[Option[T]]) {
    def as[K](implicit vc: ValueConvert[K, KeyValues]) = copy[T, K](query = query compose vc)
  }

  case class MultiQuery[T, KeyValues](query: KeyValues => F[Option[T]]) {
    def as[K](implicit vc: ValueConvert[K, KeyValues]) = copy[T, K](query = query compose vc)
  }

  case class WriteQueries[T](insert: T => F[Unit], update: (T, T) => F[Boolean], delete: T => F[Unit])

  trait RelationOperations[T, Key] {
    def tableName: String

    def fromResultSet: RSOps[Option[T]]

    def allColumns: List[ColumnName]

    def keyColumns: List[ColumnName]

    def parameters(key: Key): Iterable[QueryParam]
  }

  trait PhysicalMapping[T, Columns <: HList, ColumnsValues <: HList, Keys <: HList, SelectedKeys]
    extends DepFn1[RelationBuilder[T, Columns, ColumnsValues, Keys]] {
    type KeyValues
    type Out = DDL[RelationOperations[T, KeyValues]]
  }

  object PhysicalMapping {
    type Aux[T, Columns <: HList, ColumnsValues <: HList, Keys <: HList, SelectedKeys, KeyValues0] =
    PhysicalMapping[T, Columns, ColumnsValues, Keys, SelectedKeys] {type KeyValues = KeyValues0}
  }

  case class RelationBuilder[T, Columns <: HList, ColumnsValues <: HList, Keys <: HList]
  (baseName: String, mapper: ColumnMapper.Aux[T, Columns, ColumnsValues]) {

    def queryByKey[KeyValues <: HList]
    (implicit
     physicalMapping: PhysicalMapping.Aux[T, Columns, ColumnsValues, Keys, Keys, KeyValues]): DDL[SingleQuery[T, KeyValues]] = ???

    def queryAllByKeyColumns[K <: HList](k: K) = ???

    def writeQueries: DDL[WriteQueries[T]] = ???
  }

  //
  //    private implicit val opApp = Applicative[Option]
  //    def fromRS[ColumnsOnly <: HList, OutRS <: HList, Sequenced <: HList]
  //    (implicit values: Values.Aux[Columns, ColumnsOnly],
  //     traverser: Traverser.Aux[ColumnsOnly, columnMappingToRS.type, RSOps[OutRS]],
  //     sequenceOps: Sequencer.Aux[OutRS, Option[AllValues]]
  //    ): RSOps[Option[T]] =
  //      traverser(values(mapper.columns)).map(rs => sequenceOps(rs).map(mapper.fromValues))
  //
  //    def queryByPK[Sel <: HList](implicit selectAll: Values[Columns]) = selectAll(mapper.columns)
  //  }

  class RelationPartial[T] {

    case class RelationBuilderPartial[T, Columns <: HList, ColumnsValues <: HList]
    (baseName: String, mapper: ColumnMapper.Aux[T, Columns, ColumnsValues]) extends SingletonProductArgs {
      def key[K](k: Witness)(implicit ev: Selector[Columns, k.T]): RelationBuilder[T, Columns, ColumnsValues, k.T :: HNil] =
        RelationBuilder[T, Columns, ColumnsValues, k.T :: HNil](baseName, mapper)

      def keys(k1: Witness, k2: Witness)
              (implicit
               ev: SelectAllRecord[Columns, k1.T :: k2.T :: HNil]): RelationBuilder[T, Columns, ColumnsValues, k1.T :: k2.T :: HNil]
      = RelationBuilder[T, Columns, ColumnsValues, k1.T :: k2.T :: HNil](baseName, mapper)
    }


    def apply[Columns <: HList, ColumnsValues <: HList](name: String)(implicit gen: GenericColumnMapper.Aux[T, Columns, ColumnsValues])
    : RelationBuilderPartial[T, Columns, ColumnsValues] = RelationBuilderPartial(name, gen())
  }

  def relation[T] = new RelationPartial[T]

}
