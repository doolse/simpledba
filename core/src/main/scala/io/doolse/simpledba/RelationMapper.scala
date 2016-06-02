package io.doolse.simpledba

import cats.data.Xor
import cats.{Eval, Monad}
import io.doolse.simpledba.ValueConvert.QuestionMarks
import shapeless._
import shapeless.labelled._
import shapeless.ops.hlist.{At, Comapped, ConstMapper, Length, Mapper, Reverse, RightFolder, ToList, Zip, ZipConst, ZipWith}
import shapeless.ops.nat.ToInt
import shapeless.ops.record._
import shapeless.tag.@@

import scala.reflect.ClassTag

/**
  * Created by jolz on 10/05/16.
  */
abstract class RelationMapper[F[_] : Monad, ColumnAtom[_]] {

  type DDLStatement

  type Projection[A]
  type Where

  val stdColumnMaker = new MappingCreator[ColumnAtom, ColumnMapping] {
    def makeMapping[S, A](name: String, atom: ColumnAtom[A], get: (S) => A): ColumnMapping[S, A] = ColumnMapping[S, A](name, atom, get)

    def composer[S, S2](f: (S2) => S): ColumnComposer[ColumnMapping, S, S2] = new ColumnComposer[ColumnMapping, S, S2] {
      def apply[A](cm: ColumnMapping[S, A]): ColumnMapping[S2, A] = cm.copy[S2, A](get = cm.get compose f)
    }

    def wrapAtom[S, A](atom: ColumnAtom[A], to: (S) => A, from: (A) => S): ColumnAtom[S] = ???
  }

  trait ReadQueries {
    def selectOne[A](projection: Projection[A], where: Where): F[Option[A]]

    def selectMany[A](projection: Projection[A], where: Where, asc: Option[Boolean]): F[List[A]]
  }

  trait Projector[T, Meta, Select] extends DepFn2[PhysRelation[T], Select] {
    type A0
    type Out = Projection[A0]
  }

  object Projector {
    type Aux[T, Meta, Select, OutA] = Projector[T, Meta, Select] {
      type A0 = OutA
    }
  }

  trait KeyMapper[T, CR <: HList, KL <: HList, CVL <: HList, PKL <: HList] {
    type PartitionKey
    type SortKey
    type PartitionKeyNames
    type SortKeyNames

    def keysMapped(cm: ColumnMapper[T, CR, CVL])(name: String): PhysRelationImpl[T, PartitionKey, SortKey]
  }

  abstract class KeyMapperImpl[T, CR <: HList, KL <: HList, CVL <: HList, PKL <: HList, PKN0, PartitionKey0, SKN0, SortKey0]
    extends KeyMapper[T, CR, KL, CVL, PKL] {
    type PartitionKey = PartitionKey0
    type SortKey = SortKey0
    type PartitionKeyNames = PKN0
    type SortKeyNames = SKN0
  }

  trait PartKeyOnly[T] {
    type PartitionKey

    def wherePK(pk: PartitionKey): Where

    def selectAll: Projection[T]

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

  trait PhysRelationImpl[T, PartKey0, SortKey0] extends PhysRelation[T] {
    type PartitionKey = PartKey0
    type SortKey = SortKey0
  }

  trait ColumnListHelper[T, CVL <: HList, FullKey] {
    def materializer: ColumnMaterialzer => Option[T]

    def toPhysicalValues: T => List[PhysicalValue]

    def changeChecker: (T, T) => Option[Xor[(FullKey, List[ValueDifference]), (FullKey, FullKey, List[PhysicalValue])]]
  }

  trait ColumnListHelperBuilder[T, CR <: HList, CVL <: HList, FullKey] extends DepFn2[ColumnMapper[T, CR, CVL], CVL => FullKey] {
    type Out = ColumnListHelper[T, CVL, FullKey]
  }

  object ColumnListHelperBuilder {
    implicit def helper[T, CR <: HList, CVL <: HList, FullKey]
    (implicit allVals: PhysicalValues.Aux[CR, CVL, List[PhysicalValue]],
     differ: ValueDifferences.Aux[CR, CVL, CVL, List[ValueDifference]],
     materializeAll: MaterializeFromColumns.Aux[CR, CVL]
    ) = new ColumnListHelperBuilder[T, CR, CVL, FullKey] {
      def apply(mapper: ColumnMapper[T, CR, CVL], u: (CVL) => FullKey): ColumnListHelper[T, CVL, FullKey]
      = new ColumnListHelper[T, CVL, FullKey] {
        val columns = mapper.columns
        val toColumns = mapper.toColumns
        val fromColumns = mapper.fromColumns
        val materializer = materializeAll(columns) andThen (_.map(fromColumns))
        val colsToValues = allVals(columns)
        val toPhysicalValues = colsToValues compose toColumns

        val toKeys = u

        val changeChecker: (T, T) => Option[Xor[(FullKey, List[ValueDifference]), (FullKey, FullKey, List[PhysicalValue])]] = (existing, newValue) => {
          if (existing == newValue) None
          else Some {
            val exCols = toColumns(existing)
            val newCols = toColumns(newValue)
            val oldKey = toKeys(exCols)
            val newKey = toKeys(newCols)
            if (oldKey == newKey) Xor.left(oldKey, differ(columns, (exCols, newCols))) else Xor.right(oldKey, newKey, colsToValues(newCols))
          }
        }
      }
    }
  }

  case class ColumnMapping[S, A](name: String, atom: ColumnAtom[A], get: S => A) {
    override def toString = s"'$name' -> $atom"
  }

  trait ColumnValues[Column] {
    type Out
  }

  object ColumnValues {
    type Aux[Column, Out0] = ColumnValues[Column] {type Out = Out0}
    implicit val hnilColumnType = new ColumnValues[HNil] {
      type Out = HNil
    }

    implicit def fieldColumnType[S, K, V] = new ColumnValues[FieldType[K, ColumnMapping[S, V]]] {
      type Out = V
    }

    implicit def mappingColumnType[S, V] = new ColumnValues[ColumnMapping[S, V]] {
      type Out = V
    }

    implicit def hconsColumnType[S, H, T <: HList, TL <: HList](implicit headType: ColumnValues[H], tailTypes: ColumnValues.Aux[T, TL])
    = new ColumnValues[H :: T] {
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
     ev: Out <:< (_, N),
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

    def withCol[B](f: (A, ColumnAtom[A]) => B): B
  }

  object PhysicalValue {
    def apply[A0](_name: String, v: A0, atom: ColumnAtom[A0]): PhysicalValue = new PhysicalValue {
      type A = A0

      def name = _name

      def withCol[B](f: (A, ColumnAtom[A]) => B): B = f(v, atom)
    }
  }

  trait PhysicalValues[Columns] extends DepFn1[Columns] {
    type Value
    type OutPhys
    type Out = Value => OutPhys
  }

  trait LowPriorityPhysicalValues {
    implicit def singleValueAsList[A, CM]
    (implicit mapper: PhysicalValues.Aux[CM, A, PhysicalValue]) = new PhysicalValues.Aux[CM, A, List[PhysicalValue]] {
      def apply(u: CM) = v => List(mapper(u).apply(v))
    }
  }

  object PhysicalValues extends LowPriorityPhysicalValues {
    trait Aux[Column, Value0, Out0] extends PhysicalValues[Column] {type Value = Value0; type OutPhys = Out0}

    object physicalValue extends Poly2 {
      implicit def mapToPhysical[A, S] = at[A, ColumnMapping[S, A]] { (v, mapping) =>
        PhysicalValue(mapping.name, v, mapping.atom)
      }

      implicit def fieldValue[A, K, V](implicit c: Case.Aux[A, V, PhysicalValue])
      = at[A, FieldType[K, V]] { (v, fv) => c(HList(v, fv)): PhysicalValue }
    }

    implicit def convertToPhysical[CVL <: HList, CR <: HList, PV <: HList]
    (implicit cvl: ColumnValues.Aux[CR, CVL], zipWith: ZipWith.Aux[CVL, CR, physicalValue.type, PV],
     toList: ToList[PV, PhysicalValue]) = new Aux[CR, CVL, List[PhysicalValue]] {

      def apply(u: CR) = v => toList(zipWith(v, u))
    }

    implicit def convertSingleValue[A, CM]
    (implicit mapper: physicalValue.Case.Aux[A, CM, PhysicalValue]) = new Aux[CM, A, PhysicalValue] {
      def apply(u: CM) = t => mapper(t :: u :: HNil)
    }
  }

  trait ValueDifference {
    type A

    def name: String

    def withCol[B](f: (A, A, ColumnAtom[A]) => B): B
  }

  object ValueDifference {
    def apply[S, A0](cm: (ColumnMapping[S, A0], A0, A0)): Option[ValueDifference] = cm match {
      case (_, v1, v2) if v1 == v2 => None
      case (cm, v1, v2) => Some {
        new ValueDifference {
          val atom = cm.atom
          type A = A0

          def name = cm.name

          def withCol[B](f: (A, A, ColumnAtom[A]) => B) = f(v1, v2, atom)
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

  case class PhysicalBuilder[T, PKK, PKV, SKK, SKV](baseName: String, createPhysical: String => PhysRelationImpl[T, PKV, SKV])

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
       addKey: Updater[InKeyMap, FieldType[(T, PKK, SKK), PhysRelationImpl[T, PKV, SKV]]],
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
       addKey: Updater[InKeyMap, FieldType[(T, PKK, SKK), PhysRelationImpl[T, PKV, SKV]]],
       update: Modifier[InTypeMap, T, Already, PhysRelation[T] :: Already],
       len: Length.Aux[Already, Len],
       toInt: ToInt[Len],
       ev: LacksKey[InKeyMap, FullKey]
      )
      = at[(ReadQueryBuilder[T, PKK, PKV, SKK, SKV, Key, InRel, Out], MergedRelations[F2B, P2F]), CreatedRelations[InKeyMap, InTypeMap]]({
        case (rqb, cr) =>
          val already = selectExisting(cr.typeRel)
          val b = selectBuilder(rqb._2.full2Builder)
          val pt = b.createPhysical(s"${b.baseName}_${toInt() + 1}")
          CreatedRelations(addKey(cr.relMap, field[(T, PKK, SKK)](pt)), update(cr.typeRel, pt :: _))
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
    RelationMap <: HList, TypeRelations <: HList, AllRelations <: HList, CROUT]
    (implicit merger: RightFolder.Aux[QB, MergedRelations[HNil, HNil], mergePhysical.type, MergeOut],
     ev: MergedRelations[F2B, P2F] =:= MergeOut,
     zipWithMerged: ZipConst.Aux[MergeOut, QB, WithMerged],
     create: RightFolder.Aux[WithMerged, CreatedRelations[HNil, HNil], createRelations.type, CROUT],
     ev2: CROUT <:< CreatedRelations[RelationMap, TypeRelations],
     zipWithCreated: ZipConst.Aux[CreatedRelations[RelationMap, TypeRelations] @@ P2F, QB, WithCreated],
     allRelations: Values.Aux[RelationMap, AllRelations],
     toPhysList: ToList[AllRelations, PhysRelation[_]],
     bf: Mapper[buildQueries.type, WithCreated])
    = new QueriesBuilder[QB] {
      type QOut = bf.Out

      def apply(qb: QB) = {
        val merged = merger(qb, MergedRelations[HNil, HNil](HNil: HNil))
        val created = create(zipWithMerged(merged, qb), CreatedRelations(HNil, HNil))
        BuiltQueries(bf(zipWithCreated(tag[P2F](ev2(created)), qb)),
          Eval.later(toPhysList(allRelations(created.relMap)).map(_.createDDL))
        )
      }

    }
  }


  case class RelationDef[T, CR <: HList, KL <: HList, CVL <: HList]
  (baseName: String, mapper: ColumnMapper[T, CR, CVL])

  private object embedAll extends Poly2 {

    implicit def customAtom[S, A, E <: HList] = at[CustomAtom[S, A], ColumnMapperContext[ColumnAtom, ColumnMapping, E]] {
      case (custom, context) => context.copy(embeddedMappings = field[S](custom) :: context.embeddedMappings)
        : ColumnMapperContext[ColumnAtom, ColumnMapping, FieldType[S, CustomAtom[S, A]] :: E]
    }

    implicit def embed[A, E <: HList, C <: HList, CV <: HList](implicit gm: GenericMapping.Aux[A, ColumnAtom, ColumnMapping, E, C, CV])
    = at[Embed[A], ColumnMapperContext[ColumnAtom, ColumnMapping, E]] {
      case (_, context) => gm.embed(context)
    }
  }

  private object lookupAll extends Poly2 {
    implicit def lookupRelation[A, K <: Symbol, Keys <: HList, E <: HList, CR <: HList, CVL <: HList, CTX]
    (implicit
     gm: GenericMapping.Aux[A, ColumnAtom, ColumnMapping, E, CR, CVL], w: Witness.Aux[K])
    = at[ColumnMapperContext[ColumnAtom, ColumnMapping, E], FieldType[K, Relation[A, Keys]]](
      (context, rel) => field[K](RelationDef[A, CR, Keys, CVL](w.value.name, gm.lookup(context)))
    )
  }

  private object convertQueries extends Poly2 {
    implicit def convertPartialKey[R <: HList, K, Keys <: HList, A, CR <: HList, CVL <: HList, KL <: HList, SR, PKK, PKV, SKK, SKV]
    (implicit selRel: Selector.Aux[R, K, SR],
     ev: SR <:< RelationDef[A, CR, KL, CVL],
     keyMapper: KeyMapperImpl[A, CR, KL, CVL, Keys, PKK, PKV, SKK, SKV]
    )
    = at[R, PartialKey[K, Keys]]((rels, q) => {
      val rb = ev(selRel(rels))
      ReadQueryBuilder[A, PKK, PKV, SKK, SKV, (A, PKK),
        PartKeyOnly.Aux[A, PKV],
        MultiQuery[F, A, PKV]](PhysicalBuilder[A, PKK, PKV, SKK, SKV](rb.baseName, keyMapper.keysMapped(rb.mapper)),
        table => MultiQuery(None, { (pk, asc) =>
          val rq = table.createReadQueries
          rq.selectMany(table.selectAll, table.wherePK(pk), asc)
        }))
    })

    implicit def convertFullKey[R <: HList, K, A, PKK, PKV, SKK, SKV, SR, CR <: HList, KL <: HList, CVL <: HList]
    (implicit selRel: Selector.Aux[R, K, SR],
     ev: SR <:< RelationDef[A, CR, KL, CVL],
     keyMapper: KeyMapperImpl[A, CR, KL, CVL, KL, PKK, PKV, SKK, SKV]) = at[R, FullKey[K]]((rels, q) => {
      val rb = ev(selRel(rels))
      ReadQueryBuilder[A, PKK, PKV, SKK, SKV, (A, PKK, SKK),
        PhysRelationImpl[A, PKV, SKV],
        SingleQuery[F, A, PKV :: SKV :: HNil]](PhysicalBuilder[A, PKK, PKV, SKK, SKV](rb.baseName, keyMapper.keysMapped(rb.mapper)),
        table => SingleQuery {
          (fk: PKV :: SKV :: HNil) =>
            val w = table.whereFullKey(fk)
            val rq = table.createReadQueries
            rq.selectOne(table.selectAll, w)
        })
    })

    implicit def convertWrites[R <: HList, K, A, SR](implicit selRel: Selector.Aux[R, K, SR],
                                                     ev: SR <:< RelationDef[A, _, _, _]) = at[R, RelationWriter[K]]((rels, q) => new WriteQueryBuilder[A])
  }

  def buildModel[E <: HList, R <: HList, Q <: HList, RKL <: HList, EO <: HList, CTXO,
  ZCO <: HList, V <: HList, CRD <: HList, RDQ <: HList, QL <: HList, QOut, ER <: HList]
  (rm: RelationModel[E, R, Q])
  (implicit
   rev: Reverse.Aux[E, ER], // LeftFolder caused diverging implicits
   rf: RightFolder.Aux[ER, ColumnMapperContext[ColumnAtom, ColumnMapping, HNil], embedAll.type, CTXO],
   zipConst: ConstMapper.Aux[CTXO, R, ZCO],
   mapRelations: ZipWith.Aux[ZCO, R, lookupAll.type, CRD],
   relDefs: ConstMapper.Aux[CRD, Q, RDQ],
   mapQueries: ZipWith.Aux[RDQ, Q, convertQueries.type, QL],
   queryBuilder: QueriesBuilder.Aux[QL, QOut]
  ): BuiltQueries[QOut] = {
    val relationRecord = mapRelations(zipConst(rf(rev(rm.embedList), ColumnMapperContext(stdColumnMaker, HNil)), rm.relationRecord), rm.relationRecord)
    val queryList = mapQueries(relDefs(relationRecord, rm.queryList), rm.queryList)
    queryBuilder(queryList)
  }

  def verifyModel[E <: HList, R <: HList, Q <: HList, C2]
  (rm: RelationModel[E, R, Q], p: String => Unit = Console.err.println)
  (implicit
   vEmbed: ColumnMapperVerifier.Aux[VerifierContext[ColumnAtom, HNil], E, C2],
   vRels: ColumnMapperVerifier[C2, R])
  : BuiltQueries[QuestionMarks.type] = {
    (vEmbed.errors ++ vRels.errors).foreach(p)
    BuiltQueries[QuestionMarks.type](QuestionMarks, Eval.now(List.empty))
  }
}


