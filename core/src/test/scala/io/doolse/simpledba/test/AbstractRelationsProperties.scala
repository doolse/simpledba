package io.doolse.simpledba.test

import cats._
import org.scalacheck.Arbitrary._
import org.scalacheck.{Arbitrary, Gen, Prop}

/**
  * Created by jolz on 16/06/16.
  */
abstract class AbstractRelationsProperties[SR[-_, _], FR[-_, _], W](name: String)
    extends SimpleDBAProperties(name) with CrudProperties[SR, FR, W] {

  implicit def runProp(fa: F[Prop]): Prop = run(fa)

  def toVector[A](s: S[A]): F[Vector[A]]

  def genUpdate[A: Arbitrary](copyKey: (A, A) => A) =
    for {
      a <- arbitrary[A]
      b <- arbitrary[A]
      t <- Gen.frequency(75 -> true, 25 -> false)
    } yield {
      (a, if (t) copyKey(a, b) else b)
    }
}
