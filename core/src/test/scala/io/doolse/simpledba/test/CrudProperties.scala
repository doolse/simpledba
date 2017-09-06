package io.doolse.simpledba.test

import cats.effect.Sync
import cats.{Id, Monad, ~>}
import io.doolse.simpledba.{Flushable, WriteQueries}
import io.doolse.simpledba._
import fs2._
import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Gen, Prop, Properties}
import cats.syntax.all._

/**
  * Created by jolz on 16/06/16.
  */

object CrudProperties {
  def apply[F[_] : Monad : Sync : Flushable, A: Arbitrary, K](run: F ~> Id, writes: WriteQueries[F, A],
                                                                   findAll: A => Stream[F, A], expected: Int, genUpdate: Gen[(A, A)])
  = {
    implicit def runProp(fa: F[Prop]): Prop = run(fa)

    new Properties("CRUD ops") {
      val countAll = (a: A) => findAll(a).runLog.map(_.count(a.==))


      property("createReadDelete") = forAll { (a: A) =>
        for {
          _ <- writes.truncate >> writes.insert(a)
          count <- countAll(a)
          _ <- writes.delete(a)
          afterDel <- countAll(a)
        } yield {
          all(
            s"Expected to find $expected" |: (count ?= expected),
            "0 after delete" |: (afterDel ?= 0))
        }
      }

      property("update") = forAll(genUpdate) { case (a1, a2) =>
        for {
          _ <- writes.truncate >> writes.insert(a1)
          changed <- writes.update(a1, a2)
          countOrig <- countAll(a1)
          countNew <- countAll(a2)
        } yield {
          ("Values are different" |: (a1 != a2) ==> all(
              s"Original should be gone - $countOrig" |: countOrig == 0,
              "New values" |: countNew == expected)) ||
            ("Values are same" |: (a1 == a2) ==> all(
              "Changed flag" |: !changed,
              "Same amount after update" |: (countOrig == expected)))
        }
      }
    }
  }
}
