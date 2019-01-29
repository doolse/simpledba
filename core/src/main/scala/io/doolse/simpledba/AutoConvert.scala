package io.doolse.simpledba

import fs2.Stream
import shapeless.{::, Generic, HNil}

trait AutoConvert[A, B] extends (A => B)

object AutoConvert
{

  implicit def acSF[A, B, C, D, F[_]](implicit ctoa: AutoConvert[C, A], btod: AutoConvert[B, D]): AutoConvert[A => Stream[F,B], C => Stream[F, D]] =
    new AutoConvert[A => Stream[F,B], C => Stream[F, D]] {
      override def apply(v1: A => Stream[F, B]): C => Stream[F, D] = c => v1(ctoa(c)).map(btod)
    }

  implicit def idConv[A] : AutoConvert[A, A] = new AutoConvert[A, A] {
    def apply(a: A): A = a
  }

  implicit def oneElementOther[A, B](implicit conv: AutoConvert[A, B]) = new AutoConvert[A :: HNil, B] {
    override def apply(v1: A :: HNil): B = conv(v1.head)
  }

  implicit def oneElement[A, B](implicit convLast: AutoConvert[A, B]) = new AutoConvert[A, B :: HNil] {
    override def apply(a: A): B :: HNil = convLast(a) :: HNil
  }

  implicit def oneStreamElement[A, B, F[_]](implicit conv: AutoConvert[A, B]) = new AutoConvert[A, Stream[F, B]] {
    override def apply(a: A): Stream[F, B] = Stream.emit(conv(a))
  }

  implicit def unitConvert = new AutoConvert[HNil, Unit] {
    def apply(a: HNil) = ()
  }

  implicit def genConvert[A, Repr](implicit gen: Generic.Aux[A, Repr]) = new AutoConvert[A, Repr] {
    def apply(a: A) = gen.to(a)
  }
}
