package io.doolse.simpledba.test

import cats.syntax.all._
import cats.{Id, Monad, ~>}
import io.doolse.simpledba.{Flushable, Streamable, WriteOp, WriteQueries}
import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Gen, Prop, Properties}
import io.doolse.simpledba.syntax._

/**
  * Created by jolz on 16/06/16.
  */
trait CrudProperties[S[_[_], _], F[_]] {

  def S: Streamable[S, F]
  implicit def M: Monad[F]
  def flushable: Flushable[S, F]
  def run[A](f: F[A]): A
  def flushed(s: S[F, WriteOp]): F[Unit] = S.drain(flushable.flush(s))

  def crudProps[A: Arbitrary, K](writes: WriteQueries[S, F, A],
                                 truncate: S[F, WriteOp],
                                 findAll: A => S[F, A],
                                 expected: Int,
                                 genUpdate: Gen[(A, A)]) = {
    implicit def runProp(fa: F[Prop]): Prop = run(fa)

    new Properties("CRUD ops") {
      val countAll = (a: A) => S.toVector(findAll(a)).map(_.count(a.==))

      property("createReadDelete") = forAll { (a: A) =>
        for {
          _        <- flushed(S.append(truncate, writes.insert(a)))
          count    <- countAll(a)
          _        <- flushed(writes.delete(a))
          afterDel <- countAll(a)
        } yield {
          all(s"Expected to find $expected" |: (count ?= expected),
              "0 after delete" |: (afterDel ?= 0))
        }
      }

      property("update") = forAll(genUpdate) {
        case (a1, a2) =>
          for {
            _         <- flushed(S.append(truncate, writes.insert(a1)))
            _         <- flushed(writes.update(a1, a2))
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
