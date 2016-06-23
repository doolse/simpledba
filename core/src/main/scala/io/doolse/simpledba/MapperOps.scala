package io.doolse.simpledba

import cats.data.Xor
import shapeless.labelled._
import shapeless.ops.hlist.{At, Mapper, ToList, ToTraversable, Zip, ZipWith}
import shapeless.ops.record.{SelectAll, Selector}
import shapeless.{::, DepFn0, DepFn1, DepFn2, HList, HNil, Nat, Poly1, Poly2, record}

/**
  * Created by jolz on 8/06/16.
  */

trait ColumnsAsSeq[CR, KL, T, CA[_]] {
  type Vals
  def apply(cr: CR): Seq[ColumnMapping[CA, T, _]]
}

object ColumnsAsSeq {
  type Aux[CR, KL, T, CA[_], Out] = ColumnsAsSeq[CR, KL, T, CA] { type Vals = Out }
  implicit def extractColumns[CR <: HList, KL <: HList, T, CA[_], CRSelected <: HList]
  (implicit sa: SelectAll.Aux[CR, KL, CRSelected],
   toSeq: ToTraversable.Aux[CRSelected, Seq, ColumnMapping[CA, T, _]],
   cvl: ColumnValues[CRSelected]) = new ColumnsAsSeq[CR, KL, T, CA] {
    type Vals = cvl.Out
    def apply(cr: CR): Seq[ColumnMapping[CA, T, _]] = toSeq(sa(cr)).reverse
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

trait ColumnListHelper[CA[_], T, CVL <: HList, FullKey] {
  def materializer: ColumnMaterialzer[CA] => Option[T]

  def toPhysicalValues: T => List[PhysicalValue[CA]]

  def changeChecker: (T, T) => Option[Xor[(FullKey, List[ValueDifference[CA]]), (FullKey, FullKey, List[PhysicalValue[CA]])]]
}

trait ColumnListHelperBuilder[CA[_], T, CR <: HList, CVL <: HList, FullKey] extends DepFn2[ColumnMapper[T, CR, CVL], CVL => FullKey] {
  type Out = ColumnListHelper[CA, T, CVL, FullKey]
}

object ColumnListHelperBuilder {
  implicit def helper[CA[_], T, CR <: HList, CVL <: HList, FullKey]
  (implicit allVals: PhysicalValues.Aux[CA, CR, CVL, List[PhysicalValue[CA]]],
   differ: ValueDifferences.Aux[CA, CR, CVL, CVL, List[ValueDifference[CA]]],
   materializeAll: MaterializeFromColumns.Aux[CA, CR, CVL]
  ) = new ColumnListHelperBuilder[CA, T, CR, CVL, FullKey] {
    def apply(mapper: ColumnMapper[T, CR, CVL], u: (CVL) => FullKey): ColumnListHelper[CA, T, CVL, FullKey]
    = new ColumnListHelper[CA, T, CVL, FullKey] {
      val columns = mapper.columns
      val toColumns = mapper.toColumns
      val fromColumns = mapper.fromColumns
      val materializer = materializeAll(columns) andThen (_.map(fromColumns))
      val colsToValues = allVals(columns)
      val toPhysicalValues = colsToValues compose toColumns

      val toKeys = u

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
