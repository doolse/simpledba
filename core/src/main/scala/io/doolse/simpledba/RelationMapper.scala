package io.doolse.simpledba

import cats.Monad
import cats.data.{Reader, State, Xor}
import cats.syntax.all._
import shapeless._
import shapeless.labelled._
import shapeless.poly._
import shapeless.ops.hlist.{At, Drop, Length, Mapper, Prepend, Split, Take, ToList, Zip, ZipConst, ZipOne, ZipWith}
import shapeless.ops.product.ToHList
import shapeless.ops.record._

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by jolz on 10/05/16.
  */


abstract class RelationMapper[F[_] : Monad] {

  trait BuilderKey[T]

  class BuilderToRelations[K, V]

  implicit def btor[T] = new BuilderToRelations[BuilderKey[T], List[(PhysRelation[T], WriteQueries[T], ReadQueries)]]

  case class PhysicalTables(map: HMap[BuilderToRelations] = HMap.empty, allNeeded: Vector[(String, PhysRelation[_])] = Vector.empty)

  type DDL[A] = State[PhysicalTables, A]
  type PhysCol[A]
  type DDLStatement

  object selectStar

  trait Projection[A]

  type ProjectionT[A] <: Projection[A]
  type Where

  type PhysRelationT[T] <: PhysRelation[T]

  trait ReadQueries {
    def selectOne[A](projection: ProjectionT[A], where: Where): F[Option[A]]

    def selectMany[A](projection: ProjectionT[A], where: Where, asc: Boolean): F[List[A]]
  }

  trait WriteQueries[T] {
    self =>
    def delete(t: T): F[Unit]

    def insert(t: T): F[Unit]

    def update(existing: T, newValue: T): F[Boolean]

    def combine(other: WriteQueries[T]): WriteQueries[T] = new WriteQueries[T] {
      def delete(t: T): F[Unit] = self.delete(t) *> other.delete(t)

      def insert(t: T): F[Unit] = self.insert(t) *> other.insert(t)

      def update(existing: T, newValue: T): F[Boolean] = (self.update(existing, newValue) |@| other.update(existing, newValue)).map((a, b) => a || b)
    }
  }

  trait Projector[T, Meta, Select] extends DepFn2[PhysRelation.Aux[T, Meta, _, _], Select] {
    type A0
    type Out = ProjectionT[A0]
  }

  object Projector {
    type Aux[T, Meta, Select, OutA] = Projector[T, Meta, Select] {
      type A0 = OutA
    }
  }

  @implicitNotFound("Failed to map keys ('${PKL}') for ${T}")
  trait KeyMapper[T, CR <: HList, KL <: HList, CVL <: HList, PKL <: HList] extends DepFn1[RelationBuilder[T, CR, KL, CVL]] {
    type Meta
    type PartitionKey
    type SortKey
    type Out = PhysRelation.Aux[T, Meta, PartitionKey, SortKey]
  }

  object KeyMapper {
    type Aux[T, CR <: HList, KL <: HList, CVL <: HList, PKL <: HList, Meta0, PartitionKey0, SortKey0] = KeyMapper[T, CR, KL, CVL, PKL] {
      type Meta = Meta0
      type PartitionKey = PartitionKey0
      type SortKey = SortKey0
    }
  }

  trait PhysRelation[T] {
    type Meta
    type PartitionKey
    type SortKey
    type FullKey = PartitionKey :: SortKey :: HNil

    def createWriteQueries(tableName: String): WriteQueries[T]

    def createReadQueries(tableName: String): ReadQueries

    def createDDL(tableName: String): DDLStatement

//    def keyAsMap(key: FullKey): Map[String, Any]
//
//    def whereFromKeyMap(keyMap: Map[String, Any]): Where

    def whereFullKey(pk: FullKey): Where

    def wherePK(pk: PartitionKey): Where

    def whereRange(pk: PartitionKey, lower: SortKey, upper: SortKey): Where

    def projection[A](a: A)(implicit prj: Projector[T, Meta, A]): prj.Out
  }

  abstract class AbstractColumnListRelation[T, CR0 <: HList, CVL0 <: HList](mapper: ColumnMapper.Aux[T, CR0, CVL0])
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

  object PhysRelation {
    type Aux[T, Meta0, PartKey0, SortKey0] = PhysRelationT[T] {
      type Meta = Meta0
      type PartitionKey = PartKey0
      type SortKey = SortKey0
    }
  }

  def build[A](ddl: State[PhysicalTables, A]): A = ddl.runA(PhysicalTables()).value

  def buildSchema[A](ddl: DDL[A]) = {
    val (pt, res) = ddl.run(PhysicalTables()).value
    val tables = pt.allNeeded.map {
      case (n, pt) => pt.createDDL(n)
    }
    (tables, res)
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

  object composeLens extends Poly1 {
    implicit def convertLens[T, T2, K, A](implicit tt: TypeTag[T]) = at[(FieldType[K, ColumnMapping[T2, A]], T => T2)] {
      case (colMapping, lens) => field[K](colMapping.copy[T, A](get = colMapping.get compose lens))
    }
  }

  trait ColumnMapperT[A] {
    type Columns <: HList
    type ColumnsValues <: HList
  }

  trait ColumnMapperTAux[A, C0 <: HList, CV0 <: HList] extends ColumnMapperT[A] {
    type Columns = C0
    type ColumnsValues = CV0
  }

  @implicitNotFound("Failed to find mapper for ${A}")
  trait ColumnMapper[A] extends ColumnMapperT[A] {

    def columns: Columns

    def fromColumns: ColumnsValues => A

    def toColumns: A => ColumnsValues
  }

  object ColumnMapper {
    case class Aux[A, Columns0 <: HList, ColumnsValues0 <: HList](columns: Columns0, fromColumns: ColumnsValues0 => A,
                                                                  toColumns: A => ColumnsValues0)
      extends ColumnMapper[A] with ColumnMapperTAux[A, Columns0, ColumnsValues0]

    implicit val hnilMapper: ColumnMapper.Aux[HNil, HNil, HNil] = Aux(HNil, identity, identity)

    implicit def singleColumn[K <: Symbol, V](implicit atom: ColumnAtom[V], key: Witness.Aux[K], tt: TypeTag[FieldType[K, V]]):
    ColumnMapper.Aux[FieldType[K, V], FieldType[K, ColumnMapping[FieldType[K, V], V]] :: HNil, V :: HNil] =
      Aux(field[K](ColumnMapping[FieldType[K, V], V](key.value.name, atom, fv => fv: V)) :: HNil, v => field[K](v.head), _ :: HNil)

    implicit def multiColumn[K <: Symbol, V, Columns0 <: HList, ColumnsValues0 <: HList, CZ <: HList]
    (implicit mapping: ColumnMapper.Aux[V, Columns0, ColumnsValues0],
     zipWithLens: ZipConst.Aux[FieldType[K, V] => V, Columns0, CZ], mapper: Mapper[composeLens.type, CZ])
    = Aux[FieldType[K, V], mapper.Out, ColumnsValues0](mapper(zipWithLens(v => v: V, mapping.columns)),
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
      Aux(
        prependC(mappedHead(zipHeadLens(_.head, headMapper.columns)), mappedTail(zipTailLens(_.tail, tailMapper.columns))),
        v => headMapper.fromColumns(headVals(v)) :: tailMapper.fromColumns(tailVals(v)),
        v => prependV(headMapper.toColumns(v.head), tailMapper.toColumns(v.tail)))
      }
  }

  sealed trait GenericColumnMapper[T] extends ColumnMapperT[T] {
    val aux: ColumnMapper.Aux[T, Columns, ColumnsValues]
  }

//  trait GenericColumnMapper[T] extends ColumnMapperT[T] {
//    def apply(): ColumnMapper.Aux[T, Columns, ColumnsValues]
//  }

  object GenericColumnMapper {
    case class Aux[T, C0 <: HList, CV0 <: HList](aux: ColumnMapper.Aux[T, C0, CV0]) extends GenericColumnMapper[T] with ColumnMapperTAux[T, C0, CV0]

    def apply[T](implicit genMapper: GenericColumnMapper[T]): ColumnMapper.Aux[T, genMapper.Columns, genMapper.ColumnsValues] = genMapper.aux

    implicit def genericColumn[T, Repr <: HList, Columns0 <: HList, ColumnsValues0 <: HList, ColumnsZ <: HList]
    (implicit
     lgen: LabelledGeneric.Aux[T, Repr],
     mapping: ColumnMapper.Aux[Repr, Columns0, ColumnsValues0],
     zipWithLens: ZipConst.Aux[T => Repr, Columns0, ColumnsZ], mapper: Mapper[composeLens.type, ColumnsZ]
    ): Aux[T, mapper.Out, ColumnsValues0]
    = Aux(ColumnMapper.Aux(mapper(zipWithLens(lgen.to, mapping.columns)),
      v => lgen.from(mapping.fromColumns(v)),
      v => mapping.toColumns(lgen.to(v)))
    )
  }

  case class SingleQuery[T, KeyValues](query: KeyValues => F[Option[T]]) {
    def as[K](implicit vc: ValueConvert[K, KeyValues]) = copy[T, K](query = query compose vc)
  }

  case class MultiQuery[T, KeyValues](ascending: Boolean, queryWithOrder: (KeyValues, Boolean) => F[List[T]]) {
    def query(k: KeyValues) = queryWithOrder(k, ascending)

    def as[K](implicit vc: ValueConvert[K, KeyValues]) = copy[T, K](queryWithOrder = (k, asc) => queryWithOrder(vc(k), asc))
  }

  case class RelationBuilder[T, CR <: HList, KL <: HList, CVL <: HList]
  (baseName: String, mapper: ColumnMapper.Aux[T, CR, CVL]) extends BuilderKey[T] {
    def key: BuilderKey[T] = this

    def addRelation(physRelation: PhysRelation[T]): DDL[ReadQueries] = State { s =>
      val _relationList = s.map.get(key).getOrElse(List.empty)
      val tableName = if (_relationList.isEmpty) baseName else s"${baseName}_${_relationList.size + 1}"
      val rq = physRelation.createReadQueries(tableName)
      val relationList = (physRelation, physRelation.createWriteQueries(tableName), rq) :: _relationList
      val newMap = s.map +(key, relationList)
      (s.copy(map = newMap, allNeeded = s.allNeeded :+ tableName -> physRelation), rq)
    }

    def queryByKey[Meta, PK, SK]
    (implicit
     keyMapper: KeyMapper.Aux[T, CR, KL, CVL, KL, Meta, PK, SK],
     projector: Projector.Aux[T, Meta, selectStar.type, T])
    : DDL[SingleQuery[T, PK :: SK :: HNil]] = {
      val physTable = keyMapper(this)
      for {
        ptO <- State.inspect((s: PhysicalTables) => s.map.get(key).flatMap(_.headOption))
        rq <- ptO.map(r => Monad[DDL].pure(r._3)).getOrElse(addRelation(physTable))
      } yield {
        SingleQuery { (fk: PK :: SK :: HNil) =>
          val w = physTable.whereFullKey(fk)
          rq.selectOne(projector(physTable, selectStar), w)
        }
      }
    }


    def queryAllByKeyColumn[Meta, PartitionKey]
    (k: Witness)
    (implicit
     keyMapper: KeyMapper.Aux[T, CR, KL, CVL, k.T :: HNil, Meta, PartitionKey, _],
     projector: Projector.Aux[T, Meta, selectStar.type, T]
    ): DDL[MultiQuery[T, PartitionKey]]
    = {
      val physTable = keyMapper(this)
      addRelation(physTable).map { rq =>
        val queryWithOrder = { (pk: PartitionKey, ascending: Boolean) =>
          val w = physTable.wherePK(pk)
          rq.selectMany(projector(physTable, selectStar), w, ascending)
        }
        MultiQuery(true, queryWithOrder)
      }
    }

    def writeQueries: DDL[WriteQueries[T]] = State.inspect {
      tables => val relations = tables.map.get(key).toList.flatMap(_.map { case (_, wq, _) => wq })
        relations.reduce((l: WriteQueries[T], r: WriteQueries[T]) => l.combine(r))
    }
  }

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


