package io.doolse.simpledba

import cats.{Applicative, Functor, Monad}
import shapeless.ops.hlist.{Length, Prepend, Split}
import shapeless.{::, Generic, HList, HNil, Nat}

trait AutoConvert[A, B] extends (A => B)

trait AutoConvertLP {
  implicit def oneElementOther[A, B](implicit conv: AutoConvert[A, B]) =
    new AutoConvert[A :: HNil, B] {
      override def apply(v1: A :: HNil): B = conv(v1.head)
    }
}

object AutoConvert extends AutoConvertLP {

  implicit def idConv[A]: AutoConvert[A, A] = (a: A) => a

  implicit def fromTuple1[A]: AutoConvert[Tuple1[A], A] = (v1: Tuple1[A]) => v1._1

  implicit def toTuple1[A]: AutoConvert[A, Tuple1[A]] = (v1: A) => Tuple1(v1)

  implicit def acSF[A, B, C, D, F[_]](
      implicit F: Functor[F],
      ctoa: AutoConvert[C, A],
      btod: AutoConvert[B, D]
  ): AutoConvert[A => F[B], C => F[D]] =
    (v1: A => F[B]) => c => F.map(v1(ctoa(c)))(btod)

  implicit def oneElement[A, B, T <: HList](implicit convLast: AutoConvert[A, B],
                                            unitTail: AutoConvert[Unit, T]) =
    new AutoConvert[A, B :: T] {
      override def apply(a: A): B :: T = convLast(a) :: unitTail(())
    }

  implicit def oneStreamElement[A, B, F[_]](implicit F: Applicative[F], conv: AutoConvert[A, B]) =
    new AutoConvert[A, F[B]] {
      override def apply(a: A): F[B] = F.pure(conv(a))
    }

  implicit def unitConvert = new AutoConvert[HNil, Unit] {
    def apply(a: HNil) = ()
  }

  implicit def genToConvert[A, Repr](implicit gen: Generic.Aux[A, Repr]) = new AutoConvert[A, Repr] {
    def apply(a: A) = gen.to(a)
  }

  implicit def genFromConvert[Repr, A](implicit gen: Generic.Aux[A, Repr]) = new AutoConvert[Repr, A] {
    def apply(a: Repr) = gen.from(a)
  }

  implicit def pureConvert[A, F[_]](implicit A: Applicative[F]) = new AutoConvert[A, F[A]] {
    override def apply(v1: A): F[A] = A.pure(v1)
  }

  implicit def flattened[A, H, T1 <: HList, T2 <: HList, T <: HList, T1Len <: Nat](
      implicit
      len: Length.Aux[T1, T1Len],
      split: Split.Aux[T, T1Len, T1, T2],
      convertFlattened: AutoConvert[A, H :: T]): AutoConvert[A, (H :: T1) :: T2] =
    new AutoConvert[A, (H :: T1) :: T2] {
      override def apply(v1: A): (H :: T1) :: T2 = {
        val converted = convertFlattened(v1)
        val (t1, t2)  = split(converted.tail)
        (converted.head :: t1) :: t2
      }
    }

}
