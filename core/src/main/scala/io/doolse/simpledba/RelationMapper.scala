package io.doolse.simpledba

import cats.Monad
import cats.data.State
import shapeless._
import shapeless.labelled._
import shapeless.poly._
import shapeless.ops.hlist.{Drop, Length, Mapper, Prepend, Split, Take, ToList, ZipConst}
import shapeless.ops.product.ToHList
import shapeless.ops.record._

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by jolz on 10/05/16.
  */


abstract class RelationMapper[F[_] : Monad] {

  class BuilderToRelations[K, V]
  implicit def btor[T] = new BuilderToRelations[RelationBuilder[T], List[(PhysRelationT[T, _], WriteQueries[T])]]
  case class PhysicalTables(map: HMap[BuilderToRelations] = HMap.empty, allNeeded: List[(String, PhysRelation[_, _])] = List.empty)

  type DDL[A] = State[PhysicalTables, A]
  type PhysCol[A]
  type DDLStatement

  object selectStar

  trait Projection {
    type A
  }
  type ProjectionT <: Projection
  type Where

  type PhysRelationT[T, Meta] <: PhysRelation[T, Meta]

  trait WriteQueries[T] {
    def delete(t: T): F[Unit]
    def insert(t: T): F[Unit]
    def update(existing: T, newValue: T): F[Boolean]
    def combine(other: WriteQueries[T]): WriteQueries[T] = ???
  }

  trait Projector[T, Meta, Select] extends DepFn2[PhysRelationT[T, Meta], Select] {
    type A0
    type Out = ProjectionT {
      type A = A0
    }
  }

  object Projector {
    type Aux[T, Meta, Select, OutA] = Projector[T, Meta, Select] {
      type A0 = OutA
    }
  }

  trait KeyMapper[T, C <: HList, K <: HList, CV <: HList] extends DepFn1[RelationBuilder.Aux[T, C, K, CV]] {
    type Meta
    type Out = PhysRelation[T, Meta]
  }

  trait PhysRelation[T, Meta] {
    self: PhysRelationT[T, Meta] =>
    type PartitionKey
    type SortKey
    type FullKey = PartitionKey :: SortKey :: HNil

    def isFullKeyCompatibleWith[OPK, OSK](other: PhysRelation.Aux[T, _, OPK, OSK]): other.FullKey => FullKey
    def createReadQueries(tableName: String): WriteQueries[T]
    def createWriteQueries(tableName: String): ReadQueries
    def createDDL(tableName: String): DDLStatement

    trait ReadQueries {
      def whereFullKey(pk: FullKey): Where

      def wherePK(pk: PartitionKey): Where

      def whereRange(pk: PartitionKey, lower: SortKey, upper: SortKey): Where

      def projection[A](a: A)(implicit prj: Projector[T, Meta, A]) = prj(self, a)

      def selectOne(projection: ProjectionT, where: Where, asc: Boolean): F[Option[projection.A]]

      def selectMany(projection: ProjectionT, where: Where, asc: Boolean): F[List[projection.A]]
    }
  }

  object PhysRelation {
    type Aux[T, Meta, PartKey0, SortKey0] = PhysRelationT[T, Meta] {
      type PartitionKey = PartKey0
      type SortKey = SortKey0
    }
  }

  def build[A](ddl: State[PhysicalTables, A]): A = ddl.runA(PhysicalTables()).value

  def buildSchema[A](ddl: DDL[A]) = {
    val (pt, res) = ddl.run(PhysicalTables()).value
    val tables = pt.allNeeded.map { case (n,pt) => pt.createDDL(n) }
    (tables, res)
  }

  trait ColumnAtom[A] {
    type T
    def from: T => A
    def to: A => T
    val physicalColumn: PhysCol[T]
    def withColumn[B](v: A, f: (T, PhysCol[T]) => B): B = f(to(v), physicalColumn)
  }

  object ColumnAtom {
    implicit def stdColumn[A](implicit col: PhysCol[A], tag: ClassTag[A]) = new ColumnAtom[A] {
      type T = A
      def from = identity
      def to = identity
      val physicalColumn = col
      override def toString = tag.runtimeClass.getName
    }
  }

  case class NamedColumn[A](name: String, column: ColumnAtom[A])

  case class ColumnMapping[S, A](name: String, atom: ColumnAtom[A], get: S => A)(implicit tt: TypeTag[S]) {
    override def toString = s"('$name',$atom,$tt)"
  }

  trait ColumnValuesType[L <: HList] {
    type Out <: HList
  }

  object ColumnValuesType {
    type Aux[L <: HList, Out0 <: HList] = ColumnValuesType[L] {type Out = Out0}
    implicit val hnilColumnType = new ColumnValuesType[HNil] {
      type Out = HNil
    }

    implicit def hconsFieldColumnType[S, K, V, T <: HList](implicit tailTypes: ColumnValuesType[T])
    = new ColumnValuesType[(FieldType[K, ColumnMapping[S, V]]) :: T] {
      type Out = V :: tailTypes.Out
    }

    implicit def hconsColumnType[S, V, T <: HList](implicit tailTypes: ColumnValuesType[T])
    = new ColumnValuesType[ColumnMapping[S, V] :: T] {
      type Out = V :: tailTypes.Out
    }
  }

  trait ColumnNames[Columns <: HList] extends (Columns => List[String])

  object ColumnNames {
    implicit def columnNames[L <: HList, LM <: HList](implicit mapper: Mapper.Aux[columnNamesFromMappings.type, L, LM], toList: ToList[LM, String]) = new ColumnNames[L] {
      def apply(columns: L): List[String] = toList(mapper(columns))
    }
  }

  object columnNamesFromMappings extends Poly1 {
    implicit def mappingToName[S, A] = at[ColumnMapping[S, A]](_.name)
    implicit def fieldMappingToName[K, S, A] = at[FieldType[K, ColumnMapping[S, A]]](_.name)
  }

  object composeLens extends Poly1 {
    implicit def convertLens[T, T2, K, A](implicit tt: TypeTag[T]) = at[(FieldType[K, ColumnMapping[T2, A]], T => T2)] {
      case (colMapping, lens) => field[K](colMapping.copy[T, A](get = colMapping.get compose lens))
    }
  }

  @implicitNotFound("Failed to find mapper for ${A}")
  trait ColumnMapper[A] {
    type Columns <: HList
    type ColumnsValues <: HList

    def columns: Columns

    def fromColumns: ColumnsValues => A

    def toColumns: A => ColumnsValues
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

      def fromColumns = identity

      def toColumns = identity
    }

    implicit def singleColumn[K <: Symbol, V](implicit atom: ColumnAtom[V], key: Witness.Aux[K], tt: TypeTag[FieldType[K, V]]):
    ColumnMapper.Aux[FieldType[K, V], FieldType[K, ColumnMapping[FieldType[K, V], V]] :: HNil, V :: HNil] = new ColumnMapper[FieldType[K, V]] {
      type Columns = FieldType[K, ColumnMapping[FieldType[K, V], V]] :: HNil
      type ColumnsValues = V :: HNil

      def columns = field[K](ColumnMapping[FieldType[K, V], V](key.value.name, atom, fv => fv: V)) :: HNil

      def fromColumns = v => field[K](v.head)

      def toColumns = _ :: HNil
    }


    implicit def multiColumn[K <: Symbol, V, Columns0 <: HList, ColumnsValues0 <: HList, CZ <: HList]
    (implicit mapping: ColumnMapper.Aux[V, Columns0, ColumnsValues0],
     zipWithLens: ZipConst.Aux[FieldType[K, V] => V, Columns0, CZ], mapper: Mapper[composeLens.type, CZ]) = new ColumnMapper[FieldType[K, V]] {
      type Columns = mapper.Out
      type ColumnsValues = ColumnsValues0

      def columns = mapper(zipWithLens(v => v: V, mapping.columns))

      def fromColumns = v => field[K](mapping.fromColumns(v))

      def toColumns = mapping.toColumns
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

        def fromColumns = v => headMapper.fromColumns(headVals(v)) :: tailMapper.fromColumns(tailVals(v))

        def toColumns = v => prependV(headMapper.toColumns(v.head), tailMapper.toColumns(v.tail))
      }
    }
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
    (implicit
     lgen: LabelledGeneric.Aux[T, Repr],
     mapping: ColumnMapper.Aux[Repr, Columns0, ColumnsValues0],
     zipWithLens: ZipConst.Aux[T => Repr, Columns0, ColumnsZ], mapper: Mapper[composeLens.type, ColumnsZ]
    ): Aux[T, mapper.Out, ColumnsValues0]
    = new GenericColumnMapper[T] {
      type Columns = mapper.Out
      type ColumnsValues = ColumnsValues0

      def apply() = new ColumnMapper[T] {
        type Columns = mapper.Out
        type ColumnsValues = ColumnsValues0

        def columns = mapper(zipWithLens(lgen.to, mapping.columns))

        def fromColumns = v => lgen.from(mapping.fromColumns(v))

        def toColumns = v => mapping.toColumns(lgen.to(v))
      }
    }
  }

  case class SingleQuery[T, KeyValues](query: KeyValues => F[Option[T]]) {
    def as[K](implicit vc: ValueConvert[K, KeyValues]) = copy[T, K](query = query compose vc)
  }

  case class MultiQuery[T, KeyValues](query: KeyValues => F[List[T]]) {
    def as[K](implicit vc: ValueConvert[K, KeyValues]) = copy[T, K](query = query compose vc)
  }

  object RelationBuilder {
    type Aux[T, C0 <: HList, K0 <: HList, CV0 <: HList] = RelationBuilder[T] {
      type Columns = C0
      type Keys = K0
      type ColumnsValues = CV0
    }

    def apply[T, C0 <: HList, CV0 <: HList, K0 <: HList](_baseName: String, _mapper: ColumnMapper.Aux[T, C0, CV0], ev: SelectAll[C0, K0])
    = new RelationBuilder[T] {
      type Columns = C0
      type Keys = K0
      type ColumnsValues = CV0

      def baseName = _baseName
      def mapper = _mapper
    }
  }

  def getRelationsForBuilder[T](forBuilder: RelationBuilder[T]): DDL[List[WriteQueries[T]]] = State.inspect {
    tables => tables.map.get(forBuilder).toList.flatMap(_.map { case (_, wq) => wq })
  }

  trait RelationBuilder[T] {
    type Columns <: HList
    type Keys <: HList
    type ColumnsValues <: HList

    def baseName: String

    def mapper: ColumnMapper.Aux[T, Columns, ColumnsValues]

    def queryByKey(implicit keyMapper: KeyMapper[T, Columns, Keys, ColumnsValues]) : DDL[SingleQuery[T, ValueConvert.QuestionMarks]] = ???

    def queryAllByKeyColumn(k: Witness) : DDL[MultiQuery[T, ValueConvert.QuestionMarks]] = ???

    def writeQueries: DDL[WriteQueries[T]] = getRelationsForBuilder(this).map { relations =>
      relations.reduce((l:WriteQueries[T],r:WriteQueries[T]) => l.combine(r))
    }
  }

  class RelationPartial[T] {

    case class RelationBuilderPartial[T, Columns <: HList, ColumnsValues <: HList]
    (baseName: String, mapper: ColumnMapper.Aux[T, Columns, ColumnsValues]) extends SingletonProductArgs {
      def key(k: Witness)(implicit ev: SelectAll[Columns, k.T :: HNil]) : RelationBuilder.Aux[T, Columns, k.T :: HNil, ColumnsValues]
      = RelationBuilder(baseName, mapper, ev)
      def keys(k1: Witness, k2: Witness)(implicit ev: SelectAll[Columns, k1.T :: k2.T :: HNil]) : RelationBuilder.Aux[T, Columns, k1.T :: k2.T :: HNil, ColumnsValues]
      = RelationBuilder(baseName, mapper, ev)
    }


    def apply[Columns <: HList, ColumnsValues <: HList](name: String)(implicit gen: GenericColumnMapper.Aux[T, Columns, ColumnsValues])
    : RelationBuilderPartial[T, Columns, ColumnsValues] = RelationBuilderPartial(name, gen())
  }

  def relation[T] = new RelationPartial[T]

}

trait ValueConvert[V, L] extends (V => L)

object ValueConvert {
  trait QuestionMarks
  implicit def forDebug[V] = new ValueConvert[V, QuestionMarks] {
    override def apply(v1: V): QuestionMarks = ???
  }

  implicit def singleValue[V, L <: HList](implicit ev: (V :: HNil) =:= L) = new ValueConvert[V, L] {
    override def apply(v1: V): L = ev(v1 :: HNil)
  }

  implicit def tupleValue[V, L <: HList](implicit toList: ToHList.Aux[V, L]) = new ValueConvert[V, L] {
    override def apply(v1: V): L = toList(v1)
  }
}
