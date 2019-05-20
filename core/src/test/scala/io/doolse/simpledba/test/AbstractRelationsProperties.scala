package io.doolse.simpledba.test

import cats._
import cats.effect.Sync
import fs2._
import io.doolse.simpledba.{Flushable, WriteOp, WriteQueries}
import org.scalacheck.{Arbitrary, Gen, Prop}
import Arbitrary._

/**
  * Created by jolz on 16/06/16.
  */
abstract class AbstractRelationsProperties[F[_]: Sync](name: String)(implicit M: Monad[F])
    extends SimpleDBAProperties(name) {

  implicit def flushable: Flushable[F]

  implicit def runProp(fa: F[Prop]): Prop = run(fa)

  def crudProps[A: Arbitrary, K](wq: WriteQueries[F, A],
                                 truncate: Stream[F, WriteOp],
                                 f: A => Stream[F, A],
                                 expected: Int,
                                 genUpdate: Gen[(A, A)]) =
    CrudProperties[F, A, K](interpret, wq, truncate, f, expected, genUpdate)

  val interpret = new (F ~> Id) {
    def apply[A](fa: F[A]): Id[A] = run(fa)
  }

  def run[A](fa: F[A]): A

  def genUpdate[A: Arbitrary](copyKey: (A, A) => A) =
    for {
      a <- arbitrary[A]
      b <- arbitrary[A]
      t <- Gen.frequency(75 -> true, 25 -> false)
    } yield {
      (a, if (t) copyKey(a, b) else b)
    }
}
