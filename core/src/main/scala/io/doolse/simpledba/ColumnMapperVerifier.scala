package io.doolse.simpledba

import shapeless.labelled.FieldType
import shapeless.ops.hlist.{Selector, ZipConst}
import shapeless._

import scala.reflect.ClassTag

/**
  * Created by jolz on 2/06/16.
  */

class VerifierContext[CA[_], E]

trait ColumnMapperVerifier[Context, In] {
  type OutContext

  def errors: List[String]
}

trait ClassNames[A] {
  def names: List[String]
}


import ColumnMapperVerifier._

trait ColumnMapperVerifierLP {
  implicit def embedded[CA[_], C, CO, E, E2 <: HList, S, Repr <: HList, WC <: HList]
  (implicit lg: LabelledGeneric.Aux[S, Repr], zipWith: ZipConst.Aux[S, Repr, WC],
   verified: ColumnMapperVerifier.Aux[VerifierContext[CA, E], WC, CO]
   ,ev: CO <:< VerifierContext[CA, E2]
  )
  = successWithErrors[CA, E, S :: E2, Embed[S]](verified.errors)

  implicit def relation[C, C2, S, Keys <: HList, Repr <: HList, WC <: HList]
  (implicit lg: LabelledGeneric.Aux[S, Repr], zipWith: ZipConst.Aux[S, Repr, WC],
   verified: ColumnMapperVerifier.Aux[C, WC, C2])
  = ColumnMapperVerifier[C, Relation[S, Keys], C2](verified.errors)

  implicit def noAtomFound[CA[_], E, S, K <: Symbol, A](implicit tt: ClassTag[S], vc: ClassTag[A], k: Witness.Aux[K])
  = verError[CA, E, (FieldType[K, A], S)](
    s"${tt.runtimeClass.getName} -> ${k.value.name} : ${vc.runtimeClass.getName} has no atom or embedding")

  implicit def noAtomColumn[CA[_], E, S, A](implicit at: ClassTag[A], o: ClassTag[S])
  = verError[CA, E, Atom[S, A]](
    s"${o.runtimeClass.getName} has not atom for ${at.runtimeClass.getName}"
  )
}

object ColumnMapperVerifier extends ColumnMapperVerifierLP {
  type Aux[Context, In, ContextOut0] = ColumnMapperVerifier[Context, In] {
    type ContextOut = ContextOut0
  }
  def apply[Context, In, ContextOut0](_errors: List[String]): ColumnMapperVerifier.Aux[Context, In, ContextOut0]
  = new ColumnMapperVerifier[Context, In] {
    type ContextOut = ContextOut0

    def errors: List[String] = _errors
  }

  def verError[CA[_], E, A](str: String) = ColumnMapperVerifier[VerifierContext[CA, E], A, VerifierContext[CA, E]](List(str))

  def simpleSuccess[C, A] = ColumnMapperVerifier[C, A, C](List.empty)

  def success[CA[_], E1, E2, A] = ColumnMapperVerifier[VerifierContext[CA, E1], A, VerifierContext[CA, E2]](List.empty)

  def successWithErrors[CA[_], E1, E2, A](errors: List[String]) = ColumnMapperVerifier[VerifierContext[CA, E1], A, VerifierContext[CA, E2]](errors)

  implicit def hnilVerify[C, L <: HNil] = simpleSuccess[C, L]

  implicit def hconsVerify[C, C2, C3, H, T <: HList](implicit hv: Lazy[ColumnMapperVerifier.Aux[C, H, C2]], tv: Lazy[ColumnMapperVerifier.Aux[C2, T, C3]])
  = ColumnMapperVerifier[C, H :: T, C3](hv.value.errors ++ tv.value.errors)

  implicit def foundAtom[CA[_], E, S, K <: Symbol, A](implicit ca: CA[A])
  = success[CA, E, E, (FieldType[K, A], S)]

  implicit def foundCustomAtom[CA[_], E <: HList, S, K <: Symbol, A]
  (implicit e: Selector[E, A]) = success[CA, E, E, (FieldType[K, A], S)]

  implicit def foundUnderlyingColumn[CA[_], E <: HList, EOut <: HList, S, A]
  (implicit uc: CA[A]) = success[CA, E, S :: E, Atom[S, A]]
}

object ClassNames {
  implicit val hnil = new ClassNames[HNil] {
    def names: List[String] = List.empty
  }
  implicit def hcons[H, T <: HList](implicit hn: ClassNames[H], tn: ClassNames[T]) = new ClassNames[H :: T] {
    def names = hn.names ++ tn.names
  }
  implicit def field[K, V](implicit ct: ClassTag[K]) = new ClassNames[FieldType[K, V]] {
    def names: List[String] = List(ct.runtimeClass.getName)
  }
  implicit def vc[CA[_], E](implicit cn: ClassNames[E]) = new ClassNames[VerifierContext[CA, E]] {
    def names: List[String] = cn.names
  }
}
