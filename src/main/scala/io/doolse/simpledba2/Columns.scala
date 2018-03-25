package io.doolse.simpledba2

import cats.arrow.{Compose, Profunctor}
import shapeless.{::, Generic, HList, HNil, Nat, Witness}
import shapeless.labelled.FieldType
import shapeless.ops.hlist.{Length, Prepend, Split, ToList, ZipWithKeys}
import shapeless.ops.record.{Keys, SelectAll}
import cats.syntax.compose._
import cats.instances.function._

case class Iso[A, B](to: A => B, from: B => A)


object Iso {
  def id[A] : Iso[A, A] = Iso(identity, identity)

  implicit val composeIso : Compose[Iso] = new Compose[Iso] {
    override def compose[A, B, C](f: Iso[B, C], g: Iso[A, B]): Iso[A, C] = Iso(f.to <<< g.to, f.from >>> g.from)
  }
}

trait AutoConvert[A, B] extends (A => B)

object AutoConvert
{
  implicit def oneElement[A] = new AutoConvert[A, A :: HNil] {
    override def apply(a: A): A :: HNil = a :: HNil
  }

  implicit def unitConvert[A] = new AutoConvert[A, Unit] {
    def apply(a: A) = ()
  }

  implicit def genConvert[A, Repr](implicit gen: Generic.Aux[A, Repr]) = new AutoConvert[A, Repr] {
    def apply(a: A) = gen.to(a)
  }
}

case class ColumnSubset[C[_], R, T, Repr <: HList](columns: Seq[(String, C[_])], iso: Iso[T, Repr])

case class Columns[C[_], T, Repr <: HList](columns: Seq[(String, C[_])], iso: Iso[T, Repr])
{
  def compose[T2](ciso: Iso[T2, T]): Columns[C, T2, Repr] = copy(iso = ciso >>> iso)

  def subset[Keys](implicit ss: ColumnSubsetBuilder[Repr, Keys]): (ColumnSubset[C, Repr, ss.Out, ss.Out], Repr => ss.Out) = {
    val (_subCols, convert) = ss.apply()
    val subCols = _subCols.toSet
    (ColumnSubset(columns.filter(c => subCols(c._1)), Iso.id), convert)
  }
}

object Columns
{
  def empty[C[_]] : Columns[C, HNil, HNil] = Columns(Seq.empty, Iso.id)

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

      override def apply() = Columns(Seq.empty, Iso.id)
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
        Iso(_ :: HNil, _.head))
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
          Iso(
          t => append.apply(hc.iso.to(t.head), tc.iso.to(t.tail)),
          { o =>
            val (ho,to) = split(o)
            hc.iso.from(ho) :: tc.iso.from(to)
          }))
      }
    }
}

trait ColumnSubsetBuilder[Repr, Keys] {
  type Out <: HList
  def apply() : (List[String], Repr => Out)
}

object ColumnSubsetBuilder {
  type Aux[Repr, Keys, Out0] = ColumnSubsetBuilder[Repr, Keys] {
    type Out = Out0
  }
  implicit def isSubset[Repr <: HList, K <: HList, KOut <: HList, Out0 <: HList, RecOut <: HList]
  (implicit sa: SelectAll.Aux[Repr, K, Out0], withKeys: ZipWithKeys.Aux[K, Out0, RecOut],
   keys : Keys.Aux[RecOut, KOut],
   toList: ToList[KOut, Symbol])
  : ColumnSubsetBuilder.Aux[Repr, K, Out0] =
    new ColumnSubsetBuilder[Repr, K] {
      type Out = Out0

      def apply() = (toList(keys()).map(_.name), sa.apply)
    }
}
