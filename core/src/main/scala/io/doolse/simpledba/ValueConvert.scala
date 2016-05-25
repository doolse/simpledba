package io.doolse.simpledba

import shapeless.ops.product.ToHList
import shapeless._
import shapeless.tag.@@

trait ValueConvert[V, L] extends (V => L)

trait ValueConvertLP {
  implicit def convertCons[VH, VT <: HList, LH, LT <: HList]
  (implicit
   chead: ValueConvert[VH, LH], ctail: ValueConvert[VT, LT]) = new ValueConvert[VH :: VT, LH :: LT] {
    def apply(v1: VH :: VT) = chead(v1.head) :: ctail(v1.tail)
  }
}

object ValueConvert extends ValueConvertLP {

  trait QuestionMarks

  implicit def forDebug[V] = new ValueConvert[V, QuestionMarks] {
    override def apply(v1: V): QuestionMarks = ???
  }

  implicit def reflValue[V] = new ValueConvert[V, V] {
    def apply(v1: V): V = v1
  }


  implicit def valAsHList[V, T <: HList](implicit ct: ValueConvert[HNil, T]) = new ValueConvert[V, V :: T] {
    def apply(v1: V) = v1 :: ct(HNil)
  }


  implicit def viaHList[V, TL <: HList, L](implicit toList: ToHList.Aux[V, TL], conv: ValueConvert[TL, L]) = new ValueConvert[V, L] {
    override def apply(v1: V): L = toList(v1)
  }

  implicit def convertTagged[V, L](implicit vc: ValueConvert[V, L]) = new ValueConvert[V @@ L, L] {
    def apply(v1: V @@ L) = vc(v1)
  }

}