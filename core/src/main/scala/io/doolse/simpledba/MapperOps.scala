package io.doolse.simpledba

import cats.data.Xor
import shapeless.labelled._
import shapeless.ops.hlist.{At, IsHCons, Mapper, ToList, ToTraversable, Zip, ZipWith}
import shapeless.ops.record.{SelectAll, Selector}
import shapeless.{::, DepFn0, DepFn1, DepFn2, HList, HNil, Nat, Poly1, Poly2, record}

/**
  * Created by jolz on 8/06/16.
  */

trait ColumnsAsSeq[CR, KL, T, CA[_]] {
  type Vals
  def apply(cr: CR): (Seq[ColumnMapping[CA, T, _]], Vals => Seq[PhysicalValue[CA]])
}

object ColumnsAsSeq {
  type Aux[CR, KL, T, CA[_], Out] = ColumnsAsSeq[CR, KL, T, CA] { type Vals = Out }

  implicit def extractColumns[CR <: HList, KL <: HList, T, CA[_], CRSelected <: HList, Vals0]
  (implicit sa: SelectAll.Aux[CR, KL, CRSelected],
   toSeq: ToTraversable.Aux[CRSelected, Seq, ColumnMapping[CA, T, _]],
   pVals: PhysicalValues.Aux[CA, CRSelected, Vals0, List[PhysicalValue[CA]]]
   ) = new ColumnsAsSeq[CR, KL, T, CA] {
    type Vals = Vals0
    def apply(cr: CR) = {
      val selCols = sa(cr)
      (toSeq(selCols), pVals(selCols))
    }
  }
}

trait ColumnMaterialzer[ColumnAtom[_]] {
  def apply[A](name: String, atom: ColumnAtom[A]): Option[A]
}

trait MaterializeFromColumns[CA[_], CR] extends DepFn1[CR] {
  type OutValue
  type Out = ColumnMaterialzer[CA] => Option[OutValue]
}

object MaterializeFromColumns {
  type Aux[CA[_], CR, OutValue0] = MaterializeFromColumns[CA, CR] {
    type OutValue = OutValue0
  }
  implicit def hnil[CA[_]] = new MaterializeFromColumns[CA, HNil] {
    type OutValue = HNil

    def apply(cr: HNil) = _ => Some(HNil)
  }

  implicit def hcons[CA[_], H, T <: HList, HV, TV <: HList](implicit hm: Aux[CA, H, HV], tm: Aux[CA, T, TV]): Aux[CA, H :: T, HV :: TV] = new MaterializeFromColumns[CA, H :: T] {
    type OutValue = HV :: TV

    def apply(t: H :: T) = m => hm(t.head).apply(m).flatMap(hv => tm(t.tail).apply(m).map(tv => hv :: tv))
  }

  implicit def column[CA[_], S, V]: Aux[CA, ColumnMapping[CA, S, V], V] = new MaterializeFromColumns[CA, ColumnMapping[CA, S, V]] {
    type OutValue = V

    def apply(t: ColumnMapping[CA, S, V]) = _ (t.name, t.atom)
  }

  implicit def fieldColumn[CA[_], K, V](implicit vm: MaterializeFromColumns[CA, V]) = new MaterializeFromColumns[CA, FieldType[K, V]] {
    type OutValue = vm.OutValue

    def apply(t: FieldType[K, V]) = m => vm(t: V).apply(m)
  }
}

trait ColumnListHelper[CA[_], T, FullKey] {
  def materializer: ColumnMaterialzer[CA] => Option[T]

  def extractKey: T => FullKey

  def toPhysicalValues: T => Seq[PhysicalValue[CA]]

  def changeChecker: (T, T) => Option[Xor[(FullKey, List[ValueDifference[CA]]), (FullKey, FullKey, List[PhysicalValue[CA]])]]
}

trait ColumnListHelperBuilder[CA[_], T, CR <: HList, CVL <: HList, FullKey] extends DepFn2[ColumnMapper[T, CR, CVL], CVL => FullKey] {
  type Out = ColumnListHelper[CA, T, FullKey]
}

object ColumnListHelperBuilder {
  implicit def helper[CA[_], T, CR <: HList, CVL <: HList, FullKey]
  (implicit
   allVals: PhysicalValues.Aux[CA, CR, CVL, List[PhysicalValue[CA]]],
   differ: ValueDifferences.Aux[CA, CR, CVL, CVL, List[ValueDifference[CA]]],
   materializeAll: MaterializeFromColumns.Aux[CA, CR, CVL]
  ) = new ColumnListHelperBuilder[CA, T, CR, CVL, FullKey] {
    def apply(mapper: ColumnMapper[T, CR, CVL], toKeys: (CVL) => FullKey): ColumnListHelper[CA, T, FullKey]
    = new ColumnListHelper[CA, T, FullKey] {
      val columns = mapper.columns
      val toColumns = mapper.toColumns
      val fromColumns = mapper.fromColumns
      val materializer = materializeAll(columns) andThen (_.map(fromColumns))
      val colsToValues = allVals(columns)
      val toPhysicalValues = colsToValues compose toColumns
      val extractKey = toColumns andThen toKeys

      val changeChecker: (T, T) => Option[Xor[(FullKey, List[ValueDifference[CA]]), (FullKey, FullKey, List[PhysicalValue[CA]])]] = (existing, newValue) => {
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

trait ColumnFamilyHelper[CA[_], T, PKV, SKV] extends ColumnListHelper[CA, T, PKV :: SKV :: HNil] {
  def pkColumns: Seq[ColumnMapping[CA, T, _]]
  def skColumns: Seq[ColumnMapping[CA, T, _]]
  def physPkColumns: PKV => Seq[PhysicalValue[CA]]
  def physSkColumns: SKV => Seq[PhysicalValue[CA]]
  def toAllPhysicalValues(t: T): (Seq[PhysicalValue[CA]], Seq[PhysicalValue[CA]], Seq[PhysicalValue[CA]])
}

trait ColumnFamilyHelperBuilder[CA[_], T, CR <: HList, CVL <: HList, PKL, SKL] extends DepFn1[ColumnMapper[T, CR, CVL]] {
  type PKV
  type SKV
  type Out = ColumnFamilyHelper[CA, T, PKV, SKV]
}

object ColumnFamilyHelperBuilder {
  type Aux[CA[_], T, CR <: HList, CVL <: HList, PKL, SKL, PKV0, SKV0] = ColumnFamilyHelperBuilder[CA, T, CR, CVL, PKL, SKL] {
    type PKV = PKV0
    type SKV = SKV0
  }

  implicit def helper[CA[_], T, CR <: HList, CVL <: HList, PKL, SKL, PKV0, SKV0]
  (implicit
   allVals: PhysicalValues.Aux[CA, CR, CVL, List[PhysicalValue[CA]]],
   pkColsLookup: ColumnsAsSeq.Aux[CR, PKL, T, CA, PKV0],
   skColsLookup: ColumnsAsSeq.Aux[CR, SKL, T, CA, SKV0],
   extractor: ValueExtractor.Aux[CR, CVL, PKL :: SKL :: HNil, PKV0 :: SKV0 :: HNil],
   clHelper: ColumnListHelperBuilder[CA, T, CR, CVL, PKV0 :: SKV0 :: HNil]) = new ColumnFamilyHelperBuilder[CA, T, CR, CVL, PKL, SKL] {
    type PKV = PKV0
    type SKV = SKV0

    def apply(mapper: ColumnMapper[T, CR, CVL]): ColumnFamilyHelper[CA, T, PKV0, SKV0] = new ColumnFamilyHelper[CA, T, PKV0, SKV0] {
      val toKeys = extractor()
      val helper = clHelper(mapper, toKeys)
      val columns = mapper.columns
      val (pkColumns, physPkColumns) = pkColsLookup(columns)
      val (skColumns, physSkColumns) = skColsLookup(columns)
      val materializer = helper.materializer
      val extractKey = helper.extractKey
      val toPhysicalValues = helper.toPhysicalValues
      val changeChecker = helper.changeChecker
      val allVals1 = allVals(columns)
      def toAllPhysicalValues(t: T) = {
        val cvl = mapper.toColumns(t)
        val (pkv :: skv :: HNil) = toKeys(cvl)
        (allVals1(cvl), physPkColumns(pkv), physSkColumns(skv))
      }
    }
  }
}

trait ColumnValues[Column] {
  type Out
}

object ColumnValues {
  type Aux[Column, Out0] = ColumnValues[Column] {type Out = Out0}
  implicit val hnilColumnType = new ColumnValues[HNil] {
    type Out = HNil
  }

  implicit def fieldColumnType[CA[_], S, K, V] = new ColumnValues[FieldType[K, ColumnMapping[CA, S, V]]] {
    type Out = V
  }

  implicit def mappingColumnType[CA[_], S, V] = new ColumnValues[ColumnMapping[CA, S, V]] {
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

trait PhysicalValue[CA[_]] {
  type A
  def name: String
  def atom: CA[A]
  def v: A
}

object PhysicalValue {
  def apply[CA[_], A0](_name: String, _atom: CA[A0], _v: A0) : PhysicalValue[CA] = new PhysicalValue[CA] {
    type A = A0

    def name = _name
    def atom = _atom
    def v = _v
  }
}

trait PhysicalValues[CA[_], Columns] extends DepFn1[Columns] {
  type Value
  type OutValues
  type Out = Value => OutValues
}

object PhysicalValues {

  trait Aux[CA[_], Column, Value0, OutValues0] extends PhysicalValues[CA, Column] {
    type Value = Value0
    type OutValues = OutValues0
  }

  object physicalValue extends Poly2 {
    implicit def mapToPhysical[CA[_], A, S] = at[A, ColumnMapping[CA, S, A]] { (v, mapping) =>
      PhysicalValue(mapping.name, mapping.atom, v)
    }

    implicit def fieldValue[A, K, V, CA[_]](implicit c: Case.Aux[A, V, PhysicalValue[CA]])
    = at[A, FieldType[K, V]] { (v, fv) => c(HList(v, fv)) : PhysicalValue[CA] }
  }

  implicit def convertToPhysical[CA[_], CVL <: HList, CR <: HList, PV <: HList]
  (implicit cvl: ColumnValues.Aux[CR, CVL], zipWith: ZipWith.Aux[CVL, CR, physicalValue.type, PV],
   toList: ToList[PV, PhysicalValue[CA]]) = new Aux[CA, CR, CVL, List[PhysicalValue[CA]]] {
    def apply(u: CR) = v => toList(zipWith(v, u))
  }

  implicit def convertSingleValue[CA[_], A, CM, PV]
  (implicit mapper: physicalValue.Case.Aux[A, CM, PV], ev: PV <:< PhysicalValue[CA])= new Aux[CA, CM, A, PhysicalValue[CA]] {
    def apply(u: CM) = (v:A) => mapper(v, u)
  }
}

trait ValueDifference[CA[_]] {
  type A

  def name: String
  def existing: A
  def newValue: A
  def atom: CA[A]
}


object ValueDifference {
  def apply[CA[_], S, A0](cm: (ColumnMapping[CA, S, A0], A0, A0)): Option[ValueDifference[CA]] = cm match {
    case (_, v1, v2) if v1 == v2 => None
    case (cm, v1, v2) => Some(new ValueDifference[CA] {
      type A = A0
      def name = cm.name
      def existing = v1
      def newValue = v2
      def atom = cm.atom
    })
  }
}

trait ValueDifferences[CA[_], CR, A, B] extends DepFn2[CR, (A, B)]

object ValueDifferences {
  type Aux[CA[_], CR, A, B, Out0] = ValueDifferences[CA, CR, A, B] {type Out = Out0 }

  object valueDiff extends Poly1 {
    implicit def mapToPhysical[CA[_], S, A] = at[(ColumnMapping[CA, S, A], A, A)] (ValueDifference.apply[CA, S, A])

    implicit def fieldValue[CA[_], A, K, V](implicit c: Case.Aux[(V, A, A), Option[ValueDifference[CA]]])
    = at[(FieldType[K, V], A, A)](cv => c(cv._1: V, cv._2, cv._3) : Option[ValueDifference[CA]])
  }

  implicit def zippedDiff[CA[_], CR <: HList, VL1 <: HList, VL2 <: HList, Z <: HList]
  (implicit zipped: Zip.Aux[CR :: VL1 :: VL2 :: HNil, Z], mapper: Mapper[valueDiff.type, Z])
  : Aux[CA, CR, VL1, VL2, mapper.Out] = new ValueDifferences[CA, CR, VL1, VL2] {
    type Out = mapper.Out

    def apply(t: CR, u: (VL1, VL2)) = mapper(zipped(t :: u._1 :: u._2 :: HNil))
  }

  implicit def diffsAsList[CA[_], CR, A, B, OutL <: HList]
  (implicit vd: Aux[CA, CR, A, B, OutL], toList: ToList[OutL, Option[ValueDifference[CA]]])
  : Aux[CA, CR, A, B, List[ValueDifference[CA]]] = new ValueDifferences[CA, CR, A, B] {
    type Out = List[ValueDifference[CA]]

    def apply(t: CR, u: (A, B)) = toList(vd(t, u)).flatten
  }
}

object columnNamesFromMappings extends Poly1 {
  implicit def mappingToName[CA[_], S, A] = at[ColumnMapping[CA, S, A]](_.name)

  implicit def fieldMappingToName[CA[_], K, S, A] = at[FieldType[K, ColumnMapping[CA, S, A]]](_.name)
}

trait TableWithSK[SK]

object tablesWithSK extends Poly1 {
  implicit def withSK[CT, SK <: HList](implicit ev: CT <:< TableWithSK[SK], ishCons: IsHCons[SK]) = at[CT](identity)
}

object tablesNoSK extends Poly1 {
  implicit def noSK[CT, SK <: HList](implicit ev: CT <:< TableWithSK[HNil]) = at[CT](identity)
}

object zipWithRelation extends Poly2 {
  implicit def findRelation[K, CRD <: HList, RD, Q, T, CR <: HList, KL <: HList, CVL <: HList]
  (implicit ev: Q <:< RelationReference[K],
   s: Selector.Aux[CRD, K, RD],
   ev2: RD <:< RelationDef[T, CR, KL, CVL]) =
    at[CRD, Q] {
      (crd, q) => (q, ev2(s(crd)))
    }
}
