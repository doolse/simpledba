package io.doolse.simpledba

import shapeless.ops.product.ToHList
import shapeless._

trait ValueConvert[V, L] extends (V => L)

object ValueConvert {

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

  implicit def convertTail[V, T <: HList, T2 <: HList](implicit ct: ValueConvert[T, T2]) = new ValueConvert[V :: T, V :: T2] {
    def apply(v1: V :: T) = v1.head :: ct(v1.tail)
  }

  implicit def tupleValue[V, TL <: HList, L](implicit toList: ToHList.Aux[V, TL], conv: ValueConvert[TL, L]) = new ValueConvert[V, L] {
    override def apply(v1: V): L = toList(v1)
  }
}