package io.doolse.simpledba

import shapeless.labelled._
import shapeless.ops.hlist._
import shapeless.ops.nat.{Min, ToInt}
import shapeless.tag._
import shapeless.{HList, LabelledGeneric, Nat, Poly1, Witness}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by jolz on 8/06/16.
  */
trait ConvertVerifierContext[F[_], As[_[_]]]
case class ConvertVerifier[In, CTX](errors: In => List[String])

trait ConvertVerifierLP {
  implicit def fallbackConvert[In, CTX] = ConvertVerifier[In, CTX](i => List(i.toString()))
}

object ConvertVerifier {

  case class QueryName[In](name: String)

  trait QueryNameLP2 {
    implicit def realFallback[In] = QueryName[In]("UNKNOWN")
  }
  trait QueryNameLP extends QueryNameLP2 {
    implicit def fallback[In](implicit tt: TypeTag[In]) = QueryName[In](tt.tpe.toString)
  }
  object QueryName extends QueryNameLP {
    implicit def four[F[_], O[_[_], _, _, _], T, A, B, In]
    (implicit ev: In <:< O[F, T, A, B], ct: ClassTag[O[F, T, A, B]], ttt: TypeTag[T], tta: TypeTag[A], ttb: TypeTag[B]) = QueryName[In](
      s"${ct.runtimeClass.getSimpleName}[F, ${ttt.tpe}, ${tta.tpe}, ${ttb.tpe}]")
    implicit def three[F[_], O[_[_], _, _], T, A, In]
    (implicit ev: In <:< O[F, T, A], ct: ClassTag[O[F, T, A]], ttt: TypeTag[T], tta: TypeTag[A]) = QueryName[In](
      s"${ct.runtimeClass.getSimpleName}[F, ${ttt.tpe}, ${tta.tpe}]")
    implicit def two[F[_], O[_[_], _], T, In]
    (implicit ev: In <:< O[F, T], ct: ClassTag[O[F, T]], ttt: TypeTag[T]) = QueryName[In](
      s"${ct.runtimeClass.getSimpleName}[F, ${ttt.tpe}]")
  }

  trait conversionErrorsLP extends Poly1 {
    implicit def cantSQ[K <: Symbol, FA, FB]
    (implicit w: Witness.Aux[K], fa: QueryName[FA], fb: QueryName[FB]) = at[FA @@ FieldType[K, FB]] {
      _ => List(s"Can't convert ${fa.name} to field '${w.value.name}: ${fb.name}'")
    }
  }

  object conversionErrors extends conversionErrorsLP {
    implicit def convertible[K, A, B](implicit ev: queriesAs.Case[A @@ B]) = at[A @@ FieldType[K, B]](a => List.empty[String])
  }

  implicit def canBuild[As[_[_]], F[_],
  QLen <: Nat, QOut <: HList, QOutFirst <: HList,
  AsRepr <: HList, AsLen <: Nat, AsFirst <: HList,
  QOutTag <: HList, ErrL <: HList,
  SplitAt <: Nat]
  (implicit
   genAs: LabelledGeneric.Aux[As[F], AsRepr],
   lenAs: Length.Aux[AsRepr, AsLen],
   lenQ: Length.Aux[QOut, QLen],
   splitAt: Min.Aux[AsLen, QLen, SplitAt],
   takeQueries: Take.Aux[QOut, SplitAt, QOutFirst],
   takeAs: Take.Aux[AsRepr, SplitAt, AsFirst],
   zip: ZipWithTag.Aux[QOutFirst, AsFirst, QOutTag],
   convertErrors: Mapper.Aux[conversionErrors.type, QOutTag, ErrL],
   toList: ToList[ErrL, List[String]],
   asLen: ToInt[AsLen],
   qLen: ToInt[QLen],
   asClass: ClassTag[As[F]]
  ) = ConvertVerifier[QOut, ConvertVerifierContext[F, As]] { rm =>
    val matchedQ = takeQueries(rm)
    val diff = qLen() - asLen()
    val extra = if (diff < 0) List(s"Class $asClass contains ${Math.abs(diff)} too many entries")
    else if (diff > 0) List(s"Class $asClass is missing $diff entries") else List.empty[String]
    toList(convertErrors(zip(matchedQ))).flatten ++ extra
  }
}
