package io.doolse.simpledba.test

import cats.Monad
import cats.syntax.all._
import io.doolse.simpledba.{Streamable, WriteQueries}
import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Gen, Prop, Properties}

/**
  * Created by jolz on 16/06/16.
  */
trait CrudProperties[SR[-_, _], FR[-_, _], W] {
  type S[A] = SR[Any, A]
  type F[A] = FR[Any, A]
  type Writes[A] = WriteQueries[SR, FR, Any, W, A]
  def M : Monad[F]
  def SM : Monad[S]
  def streamable: Streamable[SR, FR]
  def run[A](f: F[A]): A
  def flush(s: S[W]): F[Unit]
  def toVector[A](s: S[A]): F[Vector[A]]

  def crudProps[A: Arbitrary, K](writes: Writes[A],
                                 truncate: S[W],
                                 findAll: A => S[A],
                                 expected: Int,
                                 genUpdate: Gen[(A, A)]) = {
    implicit def runProp(fa: F[Prop]): Prop = run(fa)

    new Properties("CRUD ops") {
      implicit val LM = M
      val countAll = (a: A) => toVector(findAll(a)).map(_.count(a.==))

      property("createReadDelete") = forAll { (a: A) =>
        for {
          _        <- flush(streamable.append(truncate, writes.insert(a)))
          count    <- countAll(a)
          _        <- flush(writes.delete(a))
          afterDel <- countAll(a)
        } yield {
          all(s"Expected to find $expected" |: (count ?= expected),
              "0 after delete" |: (afterDel ?= 0))
        }
      }

      property("update") = forAll(genUpdate) {
        case (a1, a2) =>
          for {
            _         <- flush(streamable.append(truncate, writes.insert(a1)))
            _         <- flush(writes.update(a1, a2))
            countOrig <- countAll(a1)
            countNew  <- countAll(a2)
          } yield {
            ("Values are different" |: (a1 != a2) ==> all(
              s"Original should be gone - $countOrig" |: countOrig == 0,
              "New values" |: countNew == expected)) ||
            ("Values are same" |: (a1 == a2) ==> all(
              "Same amount after update" |: (countOrig == expected)))
          }
      }
    }
  }
}
