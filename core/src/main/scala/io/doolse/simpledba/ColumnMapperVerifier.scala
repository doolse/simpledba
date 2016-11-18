package io.doolse.simpledba

import shapeless.labelled.FieldType
import shapeless.ops.hlist.{Diff, Intersection, Reify, Selector, ToList, ZipConst}
import shapeless._
import shapeless.ops.record.Keys

import scala.reflect.ClassTag

/**
  * Created by jolz on 2/06/16.
  */

trait ColumnMapperVerifierContext[CA[_], E]

case class ColumnListVerifier[Context, In](errors: List[String])

trait ColumnMapperVerifier[Context, In] {
  type OutContext

  def errors: List[String]
}

trait ClassNames[A] {
  def names: List[String]
}


trait ColumnListVerifierLP {

  implicit def noAtomFound[CTX, S, K <: Symbol, A](implicit tt: ClassTag[S], vc: ClassTag[A], k: Witness.Aux[K])
  = ColumnListVerifier[CTX, (FieldType[K, A], S)](
    List(s"${tt.runtimeClass.getName} -> ${k.value.name} : ${vc.runtimeClass.getName} has no atom or embedding"))
}

object ColumnListVerifier extends ColumnListVerifierLP {
  implicit def foundCustomAtom[CA[_], E <: HList, S, K <: Symbol, A]
  (implicit e: Selector[E, A]) = ColumnListVerifier[ColumnMapperVerifierContext[CA, E], (FieldType[K, A], S)](List.empty)

  implicit def foundAtom[CA[_], E, S, K <: Symbol, A](implicit ca: CA[A])
  = ColumnListVerifier[ColumnMapperVerifierContext[CA, E], (FieldType[K, A], S)](List.empty)

  implicit def hnilCLVerify[C, L <: HNil] = ColumnListVerifier[C, L](List.empty)

  implicit def hconsCLVerify[C, H, T <: HList](implicit hv: ColumnListVerifier[C, H], tv: ColumnListVerifier[C, T])
  = ColumnListVerifier[C, H :: T](hv.errors ++ tv.errors)
}

object ColumnMapperVerifier {
  type Aux[Context, In, ContextOut0] = ColumnMapperVerifier[Context, In] {
    type ContextOut = ContextOut0
  }
  def apply[Context, In, ContextOut0](_errors: List[String]): ColumnMapperVerifier.Aux[Context, In, ContextOut0]
  = new ColumnMapperVerifier[Context, In] {
    type ContextOut = ContextOut0

    def errors: List[String] = _errors
  }

  implicit def embedded[CA[_], C, CO, E <: HList, S, Repr <: HList, WC <: HList]
  (implicit lg: LabelledGeneric.Aux[S, Repr], zipWith: ZipConst.Aux[S, Repr, WC],
   verified: ColumnListVerifier[ColumnMapperVerifierContext[CA, E], WC]
  )
  = successWithErrors[CA, E, S :: E, Embed[S]](verified.errors)

  implicit def relation[C, S, KL <: HList, Repr <: HList, ReprKL <: HList, MissingKL <: HList, WC <: HList, K <: Symbol]
  (implicit lg: LabelledGeneric.Aux[S, Repr], zipWith: ZipConst.Aux[S, Repr, WC],
   verified: ColumnListVerifier[C, WC],
   genKeys: Keys.Aux[Repr, ReprKL], reify: Reify.Aux[KL, KL],
   diffKeys: Diff.Aux[KL, ReprKL, MissingKL], keysToList: ToList[MissingKL, Symbol],
   relName: Witness.Aux[K])
  = {
    val missingKeys = keysToList(diffKeys(reify()))
    val missingO = missingKeys.headOption.map(_ => s"Relation ${relName.value} does not contain keys - ${missingKeys.mkString(",")}").toList
    ColumnMapperVerifier[C, Relation[K, S, KL], C](missingO ++ verified.errors)
  }

  def verError[CA[_], E, A](str: String) = ColumnMapperVerifier[ColumnMapperVerifierContext[CA, E], A, ColumnMapperVerifierContext[CA, E]](List(str))

  def simpleSuccess[C, A] = ColumnMapperVerifier[C, A, C](List.empty)

  def success[CA[_], E1, E2, A] = ColumnMapperVerifier[ColumnMapperVerifierContext[CA, E1], A, ColumnMapperVerifierContext[CA, E2]](List.empty)

  def successWithErrors[CA[_], E1, E2, A](errors: List[String]) = ColumnMapperVerifier[ColumnMapperVerifierContext[CA, E1], A, ColumnMapperVerifierContext[CA, E2]](errors)

  implicit def hnilVerify[C, L <: HNil] = simpleSuccess[C, L]

  implicit def hconsVerify[C, C2, C3, H, T <: HList](implicit hv: ColumnMapperVerifier.Aux[C, H, C2], tv: ColumnMapperVerifier.Aux[C2, T, C3])
  = ColumnMapperVerifier[C, H :: T, C3](hv.errors ++ tv.errors)
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
  implicit def vc[CA[_], E](implicit cn: ClassNames[E]) = new ClassNames[ColumnMapperVerifierContext[CA, E]] {
    def names: List[String] = cn.names
  }
}
