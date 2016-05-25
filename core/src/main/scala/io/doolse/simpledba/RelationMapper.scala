package io.doolse.simpledba

import cats.{Cartesian, Eval, Monad}
import cats.data.{Reader, State, Xor}
import cats.functor.Invariant
import cats.sequence.{HListApply2, Sequencer}
import cats.syntax.all._
import shapeless._
import shapeless.labelled._
import shapeless.poly._
import shapeless.ops.hlist.{At, Drop, Length, Mapper, Prepend, RightFolder, Split, Take, ToList, Zip, ZipConst, ZipOne, ZipWith}
import shapeless.ops.nat.ToInt
import shapeless.ops.product.{ToHList, ToTuple}
import shapeless.ops.record._
import shapeless.tag.@@

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by jolz on 10/05/16.
  */


abstract class RelationMapper[F[_] : Monad] {

  type PhysCol[A]
  type DDLStatement

  trait Projection[A]

  type ProjectionT[A] <: Projection[A]
  type Where

  trait ReadQueries {
    def selectOne[A](projection: ProjectionT[A], where: Where): F[Option[A]]

    def selectMany[A](projection: ProjectionT[A], where: Where, asc: Boolean): F[List[A]]
  }

  trait Projector[T, Meta, Select] extends DepFn2[PhysRelation[T], Select] {
    type A0
    type Out = ProjectionT[A0]
  }

  object Projector {
    type Aux[T, Meta, Select, OutA] = Projector[T, Meta, Select] {
      type A0 = OutA
    }
  }

  trait ColumnMapped[A] {
    type Columns <: HList
    type ColumnsValues <: HList
  }

  object ColumnMapped {

    trait Aux[A, C0 <: HList, CV0 <: HList] extends ColumnMapped[A] {
      type Columns = C0
      type ColumnsValues = CV0
    }

    trait KeyList[A] extends ColumnMapped[A] {
      type KeyNames <: HList
    }

    trait KeyListAux[A, C0 <: HList, CV0 <: HList, KL <: HList] extends Aux[A, C0, CV0] {
      type KeyNames = KL
    }

  }


  object Column

  @implicitNotFound("Failed to map keys ('${PKL}') for ${T}")
  trait KeyMapper[T, CR <: HList, KL <: HList, CVL <: HList, PKL <: HList] {
    type PartitionKey
    type SortKey
    type PartitionKeyNames
    type SortKeyNames

    def keysMapped(cm: ColumnMapper.Aux[T, CR, CVL])(name: String): PhysRelation.Aux[T, PartitionKey, SortKey]
  }

  object KeyMapper {

    abstract class Aux[T, CR <: HList, KL <: HList, CVL <: HList, PKL <: HList, PKN0, PartitionKey0, SKN0, SortKey0]
      extends KeyMapper[T, CR, KL, CVL, PKL] {
      type PartitionKey = PartitionKey0
      type SortKey = SortKey0
      type PartitionKeyNames = PKN0
      type SortKeyNames = SKN0
    }


  }

  trait PartKeyOnly[T] {
    type PartitionKey

    def wherePK(pk: PartitionKey): Where

    def selectAll: ProjectionT[T]

    def createReadQueries: ReadQueries
  }

  object PartKeyOnly {
    type Aux[T, PK0] = PartKeyOnly[T] {
      type PartitionKey = PK0
    }
  }

  trait PhysRelation[T] extends PartKeyOnly[T] {
    type SortKey
    type FullKey = PartitionKey :: SortKey :: HNil

    def createWriteQueries: WriteQueries[F, T]

    def createDDL: DDLStatement

    def whereFullKey(pk: FullKey): Where

    def whereRange(pk: PartitionKey, lower: SortKey, upper: SortKey): Where
  }

  object PhysRelation {

    trait Aux[T, PartKey0, SortKey0] extends PhysRelation[T] {
      type PartitionKey = PartKey0
      type SortKey = SortKey0
    }

  }

  abstract class AbstractColumnListRelation[T, CR0 <: HList, CVL0 <: HList]
  (mapper: ColumnMapper.Aux[T, CR0, CVL0])
  (implicit allVals: PhysicalValues.Aux[CVL0, CR0, List[PhysicalValue]],
   differ: ValueDifferences.Aux[CR0, CVL0, CVL0, List[ValueDifference]],
   materializeAll: MaterializeFromColumns.Aux[CR0, CVL0]
  ) {
    type CR = CR0
    type CVL = CVL0
    type FullKey
    val columns: CR = mapper.columns
    val materializer: ColumnMaterialzer => Option[CVL0] = materializeAll(columns)
    val toColumns: T => CVL = mapper.toColumns
    val fromColumns: CVL => T = mapper.fromColumns
    val toPhysicalValues: CVL0 => List[PhysicalValue] = allVals(_, columns)

    def toKeys: CVL0 => FullKey

    val changeChecker: (T, T) => Option[Xor[(FullKey, List[ValueDifference]), (FullKey, FullKey, List[PhysicalValue])]] = (existing, newValue) => {
      if (existing == newValue) None
      else Some {
        val exCols = toColumns(existing)
        val newCols = toColumns(newValue)
        val oldKey = toKeys(exCols)
        val newKey = toKeys(newCols)
        if (oldKey == newKey) Xor.left(oldKey, differ(columns, (exCols, newCols))) else Xor.right(oldKey, newKey, toPhysicalValues(newCols))
      }
    }
  }


  trait ColumnAtom[A] {
    type T

    def from: T => A

    def to: A => T

    val physicalColumn: PhysCol[T]
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

  case class ColumnMapping[S, A](name: String, atom: ColumnAtom[A], get: S => A)(implicit tt: TypeTag[S]) {
    override def toString = s"'$name' -> $atom,$tt"
  }

  trait ColumnValuesType[Column] {
    type Out
  }

  object ColumnValuesType {
    type Aux[Column, Out0] = ColumnValuesType[Column] {type Out = Out0}
    implicit val hnilColumnType = new ColumnValuesType[HNil] {
      type Out = HNil
    }

    implicit def fieldColumnType[S, K, V] = new ColumnValuesType[FieldType[K, ColumnMapping[S, V]]] {
      type Out = V
    }

    implicit def mappingColumnType[S, V] = new ColumnValuesType[ColumnMapping[S, V]] {
      type Out = V
    }

    implicit def hconsColumnType[S, H, T <: HList, TL <: HList](implicit headType: ColumnValuesType[H], tailTypes: ColumnValuesType.Aux[T, TL])
    = new ColumnValuesType[H :: T] {
      type Out = headType.Out :: TL
    }
  }

  trait ColumnNames[Columns <: HList] extends (Columns => List[String])

  object ColumnNames {
    implicit def columnNames[L <: HList, LM <: HList](implicit mapper: Mapper.Aux[columnNamesFromMappings.type, L, LM], toList: ToList[LM, String]) = new ColumnNames[L] {
      def apply(columns: L): List[String] = toList(mapper(columns))
    }
  }

  trait ValueExtractor[CR <: HList, CVL <: HList, Selections] extends DepFn0 {
    type SelectionValues
    type Out = CVL => SelectionValues
  }

  object ValueExtractor {
    type Aux[CR <: HList, CVL <: HList, Selections, SV0] = ValueExtractor[CR, CVL, Selections] {
      type SelectionValues = SV0
    }

    implicit def hnilCase[CR <: HList, CVL <: HList]: Aux[CR, CVL, HNil, HNil] = new ValueExtractor[CR, CVL, HNil] {
      type SelectionValues = HNil

      def apply = _ => HNil
    }

    implicit def hconsCase[CR <: HList, CVL <: HList, H, T <: HList, TOut <: HList]
    (implicit hValues: ValueExtractor[CR, CVL, H], tValues: ValueExtractor.Aux[CR, CVL, T, TOut])
    : Aux[CR, CVL, H :: T, hValues.SelectionValues :: TOut] = new ValueExtractor[CR, CVL, H :: T] {
      type SelectionValues = hValues.SelectionValues :: TOut

      def apply = {
        val sv = hValues()
        val tv = tValues()
        cvl => sv(cvl) :: tv(cvl)
      }
    }

    implicit def fieldName[K, CM, CR <: HList, CVL <: HList, CKL <: HList, CRI <: HList, N <: Nat, V, Out]
    (implicit
     zipWithIndex: ZipValuesWithIndex.Aux[CR, CRI],
     selectCol: Selector.Aux[CRI, K, Out],
     ev: (_, N) =:= Out,
     selectIndex: At.Aux[CVL, N, V]
    ): Aux[CR, CVL, K, V]
    = {
      new ValueExtractor[CR, CVL, K] {
        type SelectionValues = V

        def apply = cvl => selectIndex(cvl)
      }
    }
  }

  trait ColumnMaterialzer {
    def apply[A](name: String, atom: ColumnAtom[A]): Option[A]
  }

  trait MaterializeFromColumns[CR] extends DepFn1[CR] {
    type OutValue
    type Out = ColumnMaterialzer => Option[OutValue]
  }

  object MaterializeFromColumns {
    type Aux[CR, OutValue0] = MaterializeFromColumns[CR] {
      type OutValue = OutValue0
    }
    implicit val hnil = new MaterializeFromColumns[HNil] {
      type CVL = HNil
      type OutValue = HNil

      def apply(cr: HNil) = _ => Some(HNil)
    }

    implicit def hcons[H, T <: HList, HV, TV <: HList](implicit hm: Aux[H, HV], tm: Aux[T, TV]): Aux[H :: T, HV :: TV] = new MaterializeFromColumns[H :: T] {
      type OutValue = HV :: TV

      def apply(t: H :: T) = m => hm(t.head).apply(m).flatMap(hv => tm(t.tail).apply(m).map(tv => hv :: tv))
    }

    implicit def column[S, V]: Aux[ColumnMapping[S, V], V] = new MaterializeFromColumns[ColumnMapping[S, V]] {
      type OutValue = V

      def apply(t: ColumnMapping[S, V]) = _ (t.name, t.atom)
    }

    implicit def fieldColumn[K, V](implicit vm: MaterializeFromColumns[V]) = new MaterializeFromColumns[FieldType[K, V]] {
      type OutValue = vm.OutValue

      def apply(t: FieldType[K, V]) = m => vm(t: V).apply(m)
    }
  }

  trait PhysicalValue {
    type A

    def name: String

    def withCol[B](f: (A, PhysCol[A]) => B): B
  }

  object PhysicalValue {
    def apply[A](_name: String, v: A, atom: ColumnAtom[A]): PhysicalValue = new PhysicalValue {
      type A = atom.T

      def name = _name

      def withCol[B](f: (A, PhysCol[A]) => B): B = f(atom.to(v), atom.physicalColumn)
    }
  }

  trait PhysicalValues[Value, Column] extends DepFn2[Value, Column]

  trait LowPriorityPhysicalValues {
    implicit def singleValueAsList[A, CM]
    (implicit mapper: PhysicalValues.Aux[A, CM, PhysicalValue]): PhysicalValues.Aux[A, CM, List[PhysicalValue]] = new PhysicalValues[A, CM] {
      type Out = List[PhysicalValue]

      def apply(t: A, u: CM) = List(mapper(t, u))
    }
  }

  object PhysicalValues extends LowPriorityPhysicalValues {
    type Aux[Value, Column, Out0] = PhysicalValues[Value, Column] {type Out = Out0}

    object physicalValue extends Poly2 {
      implicit def mapToPhysical[A, S] = at[A, ColumnMapping[S, A]] { (v, mapping) =>
        PhysicalValue(mapping.name, v, mapping.atom)
      }

      implicit def fieldValue[A, K, V](implicit c: Case.Aux[A, V, PhysicalValue]) = at[A, FieldType[K, V]] { (v, fv) => c(HList(v, fv)): PhysicalValue }
    }

    implicit def convertToPhysical[CVL <: HList, CR <: HList, PV <: HList]
    (implicit zipWith: ZipWith.Aux[CVL, CR, physicalValue.type, PV],
     toList: ToList[PV, PhysicalValue]): Aux[CVL, CR, List[PhysicalValue]] = new PhysicalValues[CVL, CR] {
      type Out = List[PhysicalValue]

      def apply(t: CVL, u: CR): List[PhysicalValue] = toList(zipWith(t, u))
    }

    implicit def convertSingleValue[A, CM]
    (implicit mapper: physicalValue.Case.Aux[A, CM, PhysicalValue]): Aux[A, CM, PhysicalValue] = new PhysicalValues[A, CM] {
      type Out = PhysicalValue

      def apply(t: A, u: CM) = mapper(t :: u :: HNil)
    }
  }

  trait ValueDifference {
    type A

    def name: String

    def withCol[B](f: (A, A, PhysCol[A]) => B): B
  }

  object ValueDifference {
    def apply[S, A0](cm: (ColumnMapping[S, A0], A0, A0)): Option[ValueDifference] = cm match {
      case (_, v1, v2) if v1 == v2 => None
      case (cm, v1, v2) => Some {
        new ValueDifference {
          val atom = cm.atom
          type A = atom.T

          def name = cm.name

          def withCol[B](f: (A, A, PhysCol[A]) => B) = f(atom.to(v1), atom.to(v2), atom.physicalColumn)
        }
      }
    }
  }

  trait ValueDifferences[CR, A, B] extends DepFn2[CR, (A, B)]

  object ValueDifferences {
    type Aux[CR, A, B, Out0] = ValueDifferences[CR, A, B] {type Out = Out0}

    object valueDiff extends Poly1 {
      implicit def mapToPhysical[S, A] = at[(ColumnMapping[S, A], A, A)](ValueDifference.apply)

      implicit def fieldValue[A, K, V, Out](implicit c: Case.Aux[(V, A, A), Option[ValueDifference]])
      = at[(FieldType[K, V], A, A)](cv => c(cv._1: V, cv._2, cv._3): Option[ValueDifference])

    }

    implicit def zippedDiff[CR <: HList, VL1 <: HList, VL2 <: HList, Z <: HList]
    (implicit zipped: Zip.Aux[CR :: VL1 :: VL2 :: HNil, Z], mapper: Mapper[valueDiff.type, Z]): Aux[CR, VL1, VL2, mapper.Out] = new ValueDifferences[CR, VL1, VL2] {
      type Out = mapper.Out

      def apply(t: CR, u: (VL1, VL2)) = mapper(zipped(t :: u._1 :: u._2 :: HNil))
    }

    implicit def diffsAsList[CR, A, B, OutL <: HList]
    (implicit vd: Aux[CR, A, B, OutL], toList: ToList[OutL, Option[ValueDifference]])
    : Aux[CR, A, B, List[ValueDifference]] = new ValueDifferences[CR, A, B] {
      type Out = List[ValueDifference]

      def apply(t: CR, u: (A, B)) = toList(vd(t, u)).flatten
    }
  }

  object columnNamesFromMappings extends Poly1 {
    implicit def mappingToName[S, A] = at[ColumnMapping[S, A]](_.name)

    implicit def fieldMappingToName[K, S, A] = at[FieldType[K, ColumnMapping[S, A]]](_.name)
  }

  @implicitNotFound("Failed to find mapper for ${A}")
  trait ColumnMapper[A] extends ColumnMapped[A] {
    def columns: Columns

    def fromColumns: ColumnsValues => A

    def toColumns: A => ColumnsValues
  }

  object composeLens extends Poly1 {
    implicit def convertLens[T, T2, K, A](implicit tt: TypeTag[T]) = at[(FieldType[K, ColumnMapping[T2, A]], T => T2)] {
      case (colMapping, lens) => field[K](colMapping.copy[T, A](get = colMapping.get compose lens))
    }
  }

  object ColumnMapper {

    class Aux[A, Columns0 <: HList, ColumnsValues0 <: HList](val columns: Columns0, val fromColumns: ColumnsValues0 => A,
                                                             val toColumns: A => ColumnsValues0)
      extends ColumnMapper[A] with ColumnMapped.Aux[A, Columns0, ColumnsValues0]

    implicit val hnilMapper: ColumnMapper.Aux[HNil, HNil, HNil] = new Aux(HNil, identity, identity)

    implicit def singleColumn[K <: Symbol, V](implicit atom: ColumnAtom[V], key: Witness.Aux[K], tt: TypeTag[FieldType[K, V]]):
    ColumnMapper.Aux[FieldType[K, V], FieldType[K, ColumnMapping[FieldType[K, V], V]] :: HNil, V :: HNil] =
      new Aux(field[K](ColumnMapping[FieldType[K, V], V](key.value.name, atom, fv => fv: V)) :: HNil, v => field[K](v.head), _ :: HNil)

    implicit def multiColumn[K <: Symbol, V, Columns0 <: HList, ColumnsValues0 <: HList, CZ <: HList]
    (implicit mapping: ColumnMapper.Aux[V, Columns0, ColumnsValues0],
     zipWithLens: ZipConst.Aux[FieldType[K, V] => V, Columns0, CZ], mapper: Mapper[composeLens.type, CZ])
    = new Aux[FieldType[K, V], mapper.Out, ColumnsValues0](mapper(zipWithLens(v => v: V, mapping.columns)),
      v => field[K](mapping.fromColumns(v)),
      mapping.toColumns)


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
      new Aux(
        prependC(mappedHead(zipHeadLens(_.head, headMapper.columns)), mappedTail(zipTailLens(_.tail, tailMapper.columns))),
        v => headMapper.fromColumns(headVals(v)) :: tailMapper.fromColumns(tailVals(v)),
        v => prependV(headMapper.toColumns(v.head), tailMapper.toColumns(v.tail)))
    }
  }

  sealed trait GenericColumnMapper[T] extends ColumnMapped[T] {
    def aux: ColumnMapper.Aux[T, Columns, ColumnsValues]
  }

  object GenericColumnMapper {

    class Aux[T, C0 <: HList, CV0 <: HList](val aux: ColumnMapper.Aux[T, C0, CV0]) extends GenericColumnMapper[T] with ColumnMapped.Aux[T, C0, CV0]

    def apply[T](implicit genMapper: GenericColumnMapper[T]): ColumnMapper.Aux[T, genMapper.Columns, genMapper.ColumnsValues] = genMapper.aux

    implicit def genericColumn[T, Repr <: HList, Columns0 <: HList, ColumnsValues0 <: HList, ColumnsZ <: HList]
    (implicit
     lgen: LabelledGeneric.Aux[T, Repr],
     mapping: ColumnMapper.Aux[Repr, Columns0, ColumnsValues0],
     zipWithLens: ZipConst.Aux[T => Repr, Columns0, ColumnsZ], mapper: Mapper[composeLens.type, ColumnsZ]
    ): Aux[T, mapper.Out, ColumnsValues0]
    = new Aux(new ColumnMapper.Aux(mapper(zipWithLens(lgen.to, mapping.columns)),
      v => lgen.from(mapping.fromColumns(v)),
      v => mapping.toColumns(lgen.to(v)))
    )
  }


  case class PhysicalBuilder[T, PKK, PKV, SKK, SKV](baseName: String, createPhysical: String => PhysRelation.Aux[T, PKV, SKV])

  case class ReadQueryBuilder[T, PKK, PKV, SKK, SKV, Key, RelRequired, Out]
  (physical: PhysicalBuilder[T, PKK, PKV, SKK, SKV],
   creatQuery: RelRequired => Out)

  class WriteQueryBuilder[T] {
    def create(tables: List[PhysRelation[T]]): WriteQueries[F, T] = {
      tables.map(_.createWriteQueries).reduce(WriteQueries.combine[F, T])
    }
  }

  implicit def singleQueryConvert[T, K1, K2](implicit vc: ValueConvert[K2, K1]) = new ValueConvert[SingleQuery[F, T, K1], SingleQuery[F, T, K2]] {
    def apply(v1: SingleQuery[F, T, K1]) = v1.as[K2]
  }

  implicit def multiQueryConvert[T, K1, K2](implicit vc: ValueConvert[K2, K1]) = new ValueConvert[MultiQuery[F, T, K1], MultiQuery[F, T, K2]] {
    def apply(v1: MultiQuery[F, T, K1]) = v1.as[K2]
  }

  case class BuiltQueries[Q](q: Q, ddl: Eval[List[DDLStatement]]) {

    def as[NQ[_[_]]] = new AsPartial[NQ]

    class AsPartial[NQ[_[_]]] {
      def apply[Repr]()(implicit gen: Generic.Aux[NQ[F], Repr], findAs: ValueConvert[Q, Repr]): NQ[F] = gen.from(findAs(q))
    }

  }

  trait QueriesBuilder[Queries] extends DepFn1[Queries] {
    type QOut
    type Out = BuiltQueries[QOut]
  }

  object QueriesBuilder {
    type Aux[Queries, QOut0] = QueriesBuilder[Queries] {type QOut = QOut0}


    case class MergedRelations[Full2Builder <: HList, Part2Full <: HList](full2Builder: Full2Builder)

    case class CreatedRelations[RelationMap <: HList, TypeRelations <: HList](relMap: RelationMap, typeRel: TypeRelations)

    trait mergePhysicalLP extends Poly2 {
      implicit def noTables[A, Full2Builder <: HList, Part2Full <: HList] = at[A, MergedRelations[Full2Builder, Part2Full]]((_, o) => o)
    }

    object mergePhysical extends mergePhysicalLP {
      implicit def readQ[T, PKK, SKK, PKV, SKV, Key, Req, Out, Full2Builder <: HList, Part2Full <: HList]
      (implicit
       ev: LacksKey[Part2Full, Key],
       update1: Updater[Full2Builder, FieldType[(T, PKK, SKK), PhysicalBuilder[T, PKK, PKV, SKK, SKV]]],
       update2: Updater[Part2Full, FieldType[Key, (T, PKK, SKK)]]
      ) = at[ReadQueryBuilder[T, PKK, PKV, SKK, SKV, Key, Req, Out], MergedRelations[Full2Builder, Part2Full]] {
        (rq, bl) => MergedRelations[update1.Out, update2.Out](update1(bl.full2Builder, field[(T, PKK, SKK)](rq.physical)))
      }
    }

    object createRelations extends Poly2 {

      implicit def writeQIgnore[T, W, Out] = at[(WriteQueryBuilder[T], W), Out]((a, b) => b)

      implicit def alreadyExisting[T, PKK, SKK, PKV, SKV, Key, InRel, Out, FullKey,
      InKeyMap <: HList, InTypeMap <: HList, F2B <: HList, P2F <: HList]
      (implicit
       fromKeyMap: Selector.Aux[P2F, Key, FullKey],
       relAlready: Selector[InKeyMap, FullKey])
      = at[(ReadQueryBuilder[T, PKK, PKV, SKK, SKV, Key, InRel, Out], MergedRelations[F2B, P2F]), CreatedRelations[InKeyMap, InTypeMap]]((a, b) => b)

      implicit def firstEntry[T, PKK, PKV, SKK, SKV, Key, InRel, Out, FullKey,
      InKeyMap <: HList, InTypeMap <: HList, F2B <: HList, P2F <: HList]
      (implicit
       fromKeyMap: Selector.Aux[P2F, Key, FullKey],
       ev: LacksKey[InKeyMap, FullKey], ev2: LacksKey[InTypeMap, T],
       selectBuilder: Selector.Aux[F2B, FullKey, PhysicalBuilder[T, PKK, PKV, SKK, SKV]],
       addKey: Updater[InKeyMap, FieldType[(T, PKK, SKK), PhysRelation.Aux[T, PKV, SKV]]],
       addNewList: Updater[InTypeMap, FieldType[T, PhysRelation[T] :: HNil]]
      )
      = at[(ReadQueryBuilder[T, PKK, PKV, SKK, SKV, Key, InRel, Out], MergedRelations[F2B, P2F]), CreatedRelations[InKeyMap, InTypeMap]]({
        case (rqb, cr) =>
          val b = selectBuilder(rqb._2.full2Builder)
          val pt = b.createPhysical(b.baseName)
          CreatedRelations(addKey(cr.relMap, field[(T, PKK, SKK)](pt)), addNewList(cr.typeRel, field[T](pt :: HNil)))
      })

      implicit def anotherEntry[T, PKK, PKV, SKK, SKV, Key, InRel, Out, FullKey,
      InKeyMap <: HList, InTypeMap <: HList, F2B <: HList, P2F <: HList, Already <: HList, Len <: Nat]
      (implicit
       fromKeyMap: Selector.Aux[P2F, Key, FullKey],
       selectBuilder: Selector.Aux[F2B, FullKey, PhysicalBuilder[T, PKK, PKV, SKK, SKV]],
       selectExisting: Selector.Aux[InTypeMap, T, Already],
       addKey: Updater[InKeyMap, FieldType[(T, PKK, SKK), PhysRelation.Aux[T, PKV, SKV]]],
       update: Updater[InTypeMap, FieldType[T, PhysRelation[T] :: Already]],
       len: Length.Aux[Already, Len],
       toInt: ToInt[Len],
       ev: LacksKey[InKeyMap, FullKey]
      )
      = at[(ReadQueryBuilder[T, PKK, PKV, SKK, SKV, Key, InRel, Out], MergedRelations[F2B, P2F]), CreatedRelations[InKeyMap, InTypeMap]]({
        case (rqb, cr) =>
          val b = selectBuilder(rqb._2.full2Builder)
          val pt = b.createPhysical(s"${b.baseName}_${toInt() + 1}")
          CreatedRelations(addKey(cr.relMap, field[(T, PKK, SKK)](pt)), update(cr.typeRel, field[T](pt :: selectExisting(cr.typeRel))))
      })
    }

    object buildQueries extends Poly1 {
      implicit def buildReader[T, PKK, PKV, SKK, SKV, Key, PhysTable, RelRequired, Out, FKey,
      InKeyMap <: HList, InTypeMap <: HList, P2F <: HList]
      (implicit
       selectFullKey: Selector.Aux[P2F, Key, FKey],
       selectTable: Selector.Aux[InKeyMap, FKey, PhysTable],
       ev: PhysTable <:< RelRequired
      )
      = at[(ReadQueryBuilder[T, PKK, PKV, SKK, SKV, Key, RelRequired, Out], CreatedRelations[InKeyMap, InTypeMap] @@ P2F)] {
        case (rqb, cr) => rqb.creatQuery(selectTable(cr.relMap))
      }

      implicit def buildWriter[T, AllTables <: HList, InKeyMap <: HList, InTypeMap <: HList, P2F <: HList]
      (implicit
       selectTables: Selector.Aux[InTypeMap, T, AllTables],
       toList: ToList[AllTables, PhysRelation[T]]
      )
      = at[(WriteQueryBuilder[T], CreatedRelations[InKeyMap, InTypeMap] @@ P2F)] {
        case (wqb, cr) => wqb.create(toList(selectTables(cr.typeRel)))
      }
    }

    implicit def buildAll[QB <: HList, MergeOut, WithMerged <: HList,
    WithCreated <: HList, P2F <: HList, F2B <: HList,
    RelationMap <: HList, TypeRelations <: HList, AllRelations <: HList]
    (implicit merger: RightFolder.Aux[QB, MergedRelations[HNil, HNil], mergePhysical.type, MergeOut],
     ev: MergedRelations[F2B, P2F] =:= MergeOut,
     zipWithMerged: ZipConst.Aux[MergeOut, QB, WithMerged],
     create: RightFolder.Aux[WithMerged, CreatedRelations[HNil, HNil], createRelations.type, CreatedRelations[RelationMap, TypeRelations]],
     zipWithCreated: ZipConst.Aux[CreatedRelations[RelationMap, TypeRelations] @@ P2F, QB, WithCreated],
     allRelations: Values.Aux[RelationMap, AllRelations],
     toPhysList: ToList[AllRelations, PhysRelation[_]],
     bf: Mapper[buildQueries.type, WithCreated])
    = new QueriesBuilder[QB] {
      type QOut = bf.Out

      def apply(qb: QB) = {
        val merged = merger(qb, MergedRelations[HNil, HNil](HNil: HNil))
        val created = create(zipWithMerged(merged, qb), CreatedRelations(HNil, HNil))
        BuiltQueries(bf(zipWithCreated(tag[P2F](created), qb)),
          Eval.later(toPhysList(allRelations(created.relMap)).map(_.createDDL))
        )
      }

    }
  }

  def build[Q, QOut <: HList](q: Q)(implicit qb: QueriesBuilder.Aux[Q, QOut]): BuiltQueries[QOut] = qb(q)

  def queryByFullKey[T, CR <: HList, KL <: HList, CVL <: HList, PKK, PKV, SKK, SKV]
  (rb: RelationBuilder[T, CR, KL, CVL])
  (implicit
   keyMapper: KeyMapper.Aux[T, CR, KL, CVL, KL, PKK, PKV, SKK, SKV]
  ) = ReadQueryBuilder[T, PKK, PKV, SKK, SKV, (T, PKK, SKK),
    PhysRelation.Aux[T, PKV, SKV],
    SingleQuery[F, T, PKV :: SKV :: HNil]](PhysicalBuilder[T, PKK, PKV, SKK, SKV](rb.baseName, keyMapper.keysMapped(rb.mapper)),
    table => SingleQuery {
      (fk: PKV :: SKV :: HNil) =>
        val w = table.whereFullKey(fk)
        val rq = table.createReadQueries
        rq.selectOne(table.selectAll, w)
    })

  def queryAllByKeyColumn[T, CR <: HList, KL <: HList, CVL <: HList, PKK, PKV, SKK, SKV]
  (k: Witness, rb: RelationBuilder[T, CR, KL, CVL])
  (implicit
   keyMapper: KeyMapper.Aux[T, CR, KL, CVL, k.T :: HNil, PKK, PKV, SKK, SKV]
  ) = ReadQueryBuilder[T, PKK, PKV, SKK, SKV, (T, PKK),
    PartKeyOnly.Aux[T, PKV],
    MultiQuery[F, T, PKV]](PhysicalBuilder[T, PKK, PKV, SKK, SKV](rb.baseName, keyMapper.keysMapped(rb.mapper)),
    table => MultiQuery(true, { (pk, asc) =>
      val rq = table.createReadQueries
      rq.selectMany(table.selectAll, table.wherePK(pk), asc)
    }))

  def writeQueries[T, CR <: HList, KL <: HList, CVL <: HList]
  (rb: RelationBuilder[T, CR, KL, CVL])
  = new WriteQueryBuilder[T]


  case class RelationBuilder[T, CR <: HList, KL <: HList, CVL <: HList]
  (baseName: String, mapper: ColumnMapper.Aux[T, CR, CVL])

  class RelationPartial[T] {

    case class RelationBuilderPartial[T, Columns <: HList, ColumnsValues <: HList]
    (baseName: String, mapper: ColumnMapper.Aux[T, Columns, ColumnsValues]) extends SingletonProductArgs {
      def key(k: Witness)(implicit ev: SelectAll[Columns, k.T :: HNil]): RelationBuilder[T, Columns, k.T :: HNil, ColumnsValues]
      = RelationBuilder(baseName, mapper)

      def keys(k1: Witness, k2: Witness)(implicit ev: SelectAll[Columns, k1.T :: k2.T :: HNil]): RelationBuilder[T, Columns, k1.T :: k2.T :: HNil, ColumnsValues]
      = RelationBuilder(baseName, mapper)
    }


    def apply(name: String)(implicit gen: GenericColumnMapper[T])
    : RelationBuilderPartial[T, gen.Columns, gen.ColumnsValues] = RelationBuilderPartial(name, gen.aux)
  }

  def relation[T] = new RelationPartial[T]

}


