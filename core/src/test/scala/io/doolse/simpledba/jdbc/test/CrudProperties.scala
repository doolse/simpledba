package io.doolse.simpledba.jdbc.test

import cats.effect.Sync
import cats.syntax.all._
import cats.{Id, Monad, ~>}
import fs2._
import io.doolse.simpledba.{Flushable, WriteOp, WriteQueries}
import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Gen, Prop, Properties}
import io.doolse.simpledba.syntax._

/**
  * Created by jolz on 16/06/16.
  */

object CrudProperties {
  def apply[F[_] : Monad : Sync : Flushable, A: Arbitrary, K](run: F ~> Id, writes: WriteQueries[F, A],  truncate: Stream[F, WriteOp],
                                                                   findAll: A => Stream[F, A], expected: Int, genUpdate: Gen[(A, A)])
  = {
    implicit def runProp(fa: F[Prop]): Prop = run(fa)

    def flushed(s: Stream[F, WriteOp]): F[Unit] = s.flush.compile.drain

    new Properties("CRUD ops") {
      val countAll = (a: A) => findAll(a).compile.toVector.map(_.count(a.==))


      property("createReadDelete") = forAll { (a: A) =>
        for {
          _ <- flushed(truncate ++ writes.insert(a))
          count <- countAll(a)
          _ <- flushed(writes.delete(a))
          afterDel <- countAll(a)
        } yield {
          all(
            s"Expected to find $expected" |: (count ?= expected),
            "0 after delete" |: (afterDel ?= 0))
        }
      }

      property("update") = forAll(genUpdate) { case (a1, a2) =>
        for {
          _ <- flushed(truncate ++ writes.insert(a1))
          _ <- flushed(writes.update(a1, a2))
          countOrig <- countAll(a1)
          countNew <- countAll(a2)
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
