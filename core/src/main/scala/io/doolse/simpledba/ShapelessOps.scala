package io.doolse.simpledba

import shapeless.PolyDefns.Case
import shapeless._
import shapeless.labelled.FieldType
import shapeless.labelled.field
import shapeless.ops.hlist.ZipWithKeys
import shapeless.ops.record.SelectAll
import shapeless.tag.@@
import shapeless.{DepFn1, DepFn2, HList, HNil, Nat, Succ}
import poly._

/**
  * Zip a HList with another HList, tagging the left with the right.
  * L @@ R
  *
  * @author Jolse Maginnis
  */
trait ZipWithTag[L <: HList, R <: HList] extends DepFn1[L] with Serializable { type Out <: HList }

object ZipWithTag {
  type Aux[L <: HList, R <: HList, Out0 <: HList] = ZipWithTag[L, R] { type Out = Out0 }

  implicit val hnil : Aux[HNil, HNil, HNil] = new ZipWithTag[HNil, HNil] {
    type Out = HNil

    def apply(t: HNil) = HNil
  }

  implicit def hcons[LH, RH, LT <: HList, RT <: HList]
  (implicit tailZipper: ZipWithTag[LT, RT])
  : Aux[LH :: LT, RH :: RT, (LH @@ RH) :: tailZipper.Out] = new ZipWithTag[LH :: LT, RH :: RT] {
    type Out = (LH @@ RH) :: tailZipper.Out

    def apply(t: LH :: LT) = tag[RH](t.head) :: tailZipper(t.tail)
  }
}

/**
  * Zip the values with their index.
  * FieldType[K, (V, Index)]
  *
  * @author Jolse Maginnis
  */
trait ZipValuesWithIndex[L <: HList] extends DepFn1[L] with Serializable { type Out <: HList }

object ZipValuesWithIndex {
  type Aux[L <: HList, Out0] = ZipValuesWithIndex[L] { type Out = Out0 }


  trait Helper[L <: HList, N <: Nat] extends DepFn2[L, N] with Serializable {
    type Out <: HList
  }

  object Helper {

    type Aux[L <: HList, N <: Nat, Out0 <: HList] = Helper[L, N] { type Out = Out0 }

    implicit def hnil[N <: Nat]: Aux[HNil, N, HNil] = new Helper[HNil, N] {
      type Out = HNil

      def apply(t: HNil, u: N) = HNil
    }

    implicit def hcons[K, V, T <: HList, N <: Nat]
    (implicit tailHelper: Helper[T, Succ[N]]): Aux[FieldType[K, V] :: T, N, FieldType[K, (V, N)] :: tailHelper.Out]
    = new Helper[FieldType[K, V] :: T, N] {
      type Out = FieldType[K, (V, N)] :: tailHelper.Out
      def apply(t: FieldType[K, V] :: T, u: N) = field[K]((t.head:V, u)) :: tailHelper(t.tail, Succ[N]())
    }
  }

  implicit def useHelper[L <: HList](implicit h: Helper[L, _0]): Aux[L, h.Out] = new ZipValuesWithIndex[L] {
    type Out = h.Out

    def apply(t: L) = h(t, Nat._0)
  }
}

/**
  * Select fields as another record.
  *
  * @author Jolse Maginnis
  */
@annotation.implicitNotFound(msg = "No fields ${K} in record ${L}")
trait SelectAllRecord[L <: HList, K <: HList] extends DepFn1[L] with Serializable { type Out <: HList }

object SelectAllRecord {
  def apply[L <: HList, K <: HList](implicit sa: SelectAllRecord[L, K]): Aux[L, K, sa.Out] = sa

  type Aux[L <: HList, K <: HList, Out0 <: HList] = SelectAllRecord[L, K] { type Out = Out0 }

  implicit def selectAllRecord[L <: HList, K <: HList, SV <: HList](implicit
                                                                    sa: SelectAll.Aux[L, K, SV],
                                                                    zwk: ZipWithKeys[K, SV]
                                                                   ): Aux[L, K, zwk.Out] = new SelectAllRecord[L, K] {
    type Out = zwk.Out

    def apply(t: L): Out = zwk(sa(t))
  }
}

trait Values2[L <: HList] extends DepFn1[L] with Serializable { type Out <: HList }

object Values2 {
  type Aux[L <: HList, Out0 <: HList] = Values2[L] { type Out = Out0 }

  implicit def hnilValues[L <: HNil]: Aux[L, HNil] =
    new Values2[L] {
      type Out = HNil
      def apply(l: L): Out = HNil
    }

  implicit def hlistValues[K, K2, K3, V, T <: HList](implicit vt: Values2[T]): Aux[FieldType[(K, K2, K3), V] :: T, V :: vt.Out] =
    new Values2[FieldType[(K, K2, K3), V] :: T] {
      type Out = V :: vt.Out
      def apply(l: FieldType[(K, K2, K3), V] :: T): Out = (l.head: V) :: vt(l.tail)
    }
}

class WitnessList[A, MT[_, A] <: DepFn1[A]](a: A) {
  def apply(w: Witness)(implicit f: MT[w.T :: HNil, A]) = f(a)
  def apply(w1: Witness, w2: Witness)(implicit f: MT[w1.T :: w2.T :: HNil, A]) = f(a)
  def apply(w1: Witness, w2: Witness, w3: Witness)(implicit f: MT[w1.T :: w2.T :: w3.T :: HNil, A]) = f(a)
}
