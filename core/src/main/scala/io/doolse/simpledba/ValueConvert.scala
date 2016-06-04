package io.doolse.simpledba

import io.doolse.simpledba.ValueConvert.QuestionMarks
import shapeless.ops.product.ToHList
import shapeless._
import shapeless.tag.@@

trait ValueConvert[V, L] extends (V => L)

trait ValueConvertLP {
  implicit def consConvert[VH, VT <: HList, TH, TT <: HList](implicit hvc: ValueConvert[VH, TH], ct: ValueConvert[VT, TT]) = new ValueConvert[VH :: VT, TH :: TT] {
    def apply(v1: VH :: VT) = hvc(v1.head) :: ct(v1.tail)
  }

  implicit def withTrailingHNil[V, H, T <: HList](implicit vc: ValueConvert[V, H], vn: ValueConvert[HNil, T], nev: V =:!= QuestionMarks.type) = new ValueConvert[V, H :: T] {
    def apply(v1: V) = vc(v1) :: vn(HNil)
  }
}

object ValueConvert extends ValueConvertLP {

  object QuestionMarks

  implicit def forDebug[V] = new ValueConvert[QuestionMarks.type, V] {
    def apply(v1: QuestionMarks.type): V = sys.error("Just for debugging")
  }

  implicit def reflValue[V] = new ValueConvert[V, V] {
    def apply(v1: V): V = v1
  }

  implicit def convertTagged[V, L](implicit vc: ValueConvert[V, L]) = new ValueConvert[V @@ L, L] {
    def apply(v1: V @@ L) = vc(v1)
  }

  implicit def stripHNil[V] = new ValueConvert[V :: HNil, V] {
    def apply(v1: V :: HNil): V = v1.head
  }

  implicit def viaHList[V, VL <: HList, L](implicit toHList: ToHList.Aux[V, VL], conv: ValueConvert[VL, L]) = new ValueConvert[V, L] {
    def apply(v1: V): L = conv(toHList(v1))
  }
}
