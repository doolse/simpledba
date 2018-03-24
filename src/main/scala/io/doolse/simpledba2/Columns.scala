package io.doolse.simpledba2

import shapeless.{::, HList, HNil, Nat, Witness}
import shapeless.labelled.FieldType
import shapeless.ops.hlist.{Length, Prepend, Split, ToList, ZipWithKeys}
import shapeless.ops.record.{Keys, SelectAll}

case class Columns[C[_], T, Repr <: HList](columns: Seq[(String, C[_])], to: T => Repr, from: Repr => T)
{
  def isomap[T2](to2: T => T2, from2: T2 => T) : Columns[C, T2, Repr] =
    Columns(columns, to = from2.andThen(to), from = from.andThen(to2))

  def subset[Keys](implicit ss: ColumnSubset[Repr, Keys]): (Columns[C, ss.Out, ss.Out], Repr => ss.Out) = {
    val (_subCols, convert) = ss.apply()
    val subCols = _subCols.toSet
    (Columns(columns.filter(c => subCols(c._1)), identity, identity), convert)
  }
}

object Columns
{
  def empty[C[_]] : Columns[C, HNil, HNil] = Columns(Seq.empty, identity, identity)
}

trait ColumnBuilder[C[_], T] {
  type Repr <: HList
  def apply() : Columns[C, T, Repr]
}


object ColumnBuilder {
  type Aux [C[_], T, Repr0 <: HList] = ColumnBuilder[C, T]
    {
      type Repr = Repr0
    }

  implicit def hnilRelation[C[_]]: ColumnBuilder.Aux[C, HNil, HNil] =
    new ColumnBuilder[C, HNil] {
      type Repr = HNil

      override def apply() = Columns(Seq.empty, identity, identity)
    }

  implicit def embeddedField[C[_], K, V, Repr <: HList]
  (implicit embeddedCols: ColumnBuilder.Aux[C, V, Repr]) : ColumnBuilder.Aux[C, FieldType[K, V], Repr]
  = embeddedCols.asInstanceOf[ColumnBuilder.Aux[C, FieldType[K, V], Repr]]

  implicit def singleColumn[C[_], K <: Symbol, V]
  (implicit wk: Witness.Aux[K], headColumn: C[V])
  : ColumnBuilder.Aux[C, FieldType[K, V], FieldType[K, V] :: HNil]
  = new ColumnBuilder[C, FieldType[K, V]] {
    type Repr = FieldType[K, V] :: HNil

    override def apply() =
      Columns(Seq(wk.value.name -> headColumn),
        _ :: HNil, _.head)
  }

  implicit def hconsRelation[C[_], H, HOut <: HList,
  HLen <: Nat, T <: HList, TOut <: HList, Out <: HList]
  (implicit
   headColumns: ColumnBuilder.Aux[C, H, HOut],
   tailColumns: ColumnBuilder.Aux[C, T, TOut],
   len: Length.Aux[HOut, HLen],
   append: Prepend.Aux[HOut, TOut, Out],
   split: Split.Aux[Out, HLen, HOut, TOut]
  ) : ColumnBuilder.Aux[C, H :: T, Out] =
    new ColumnBuilder[C, H :: T] {
      type Repr = Out

      override def apply() = {
        val hc = headColumns.apply()
        val tc = tailColumns.apply()
        Columns(hc.columns ++ tc.columns,
          t => append.apply(hc.to(t.head), tc.to(t.tail)),
          { o =>
            val (ho,to) = split(o)
            hc.from(ho) :: tc.from(to)
          })
      }
    }
}

trait ColumnSubset[Repr, Keys] {
  type Out <: HList
  def apply() : (List[String], Repr => Out)
}

object ColumnSubset {
  type Aux[Repr, Keys, Out0] = ColumnSubset[Repr, Keys] {
    type Out = Out0
  }
  implicit def isSubset[Repr <: HList, K <: HList, KOut <: HList, Out0 <: HList, RecOut <: HList]
  (implicit sa: SelectAll.Aux[Repr, K, Out0], withKeys: ZipWithKeys.Aux[K, Out0, RecOut],
   keys : Keys.Aux[RecOut, KOut],
   toList: ToList[KOut, Symbol])
  : ColumnSubset.Aux[Repr, K, Out0] =
    new ColumnSubset[Repr, K] {
      type Out = Out0

      def apply() = (toList(keys()).map(_.name), sa.apply)
    }
}
