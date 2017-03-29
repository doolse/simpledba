package io.doolse.simpledba.test

import cats._
import fs2._
import fs2.util.Catchable
import io.doolse.simpledba.{Flushable, WriteQueries}
import org.scalacheck.Arbitrary._
import org.scalacheck.{Arbitrary, Gen, Prop, Properties}

/**
  * Created by jolz on 16/06/16.
  */
abstract class AbstractRelationsProperties[F[_] : Catchable : Flushable](name: String)(implicit M: Monad[F]) extends SimpleDBAProperties(name) {
  implicit def runProp(fa: F[Prop]): Prop = run(fa)

  def crudProps[A : Arbitrary, K](wq: WriteQueries[F, A], f: A => Stream[F, A], expected: Int, genUpdate: Gen[(A, A)]) =
    CrudProperties[F, A, K](interpret, wq, f, expected, genUpdate)

  val interpret = new (F ~> Id) {
    def apply[A](fa: F[A]): Id[A] = run(fa)
  }

  def run[A](fa: F[A]): A

  def genUpdate[A: Arbitrary](copyKey: (A, A) => A) = for {
    a <- arbitrary[A]
    b <- arbitrary[A]
    t <- Gen.frequency(75 -> true, 25 -> false)
  } yield {
    (a, if (t) copyKey(a, b) else b)
  }
}