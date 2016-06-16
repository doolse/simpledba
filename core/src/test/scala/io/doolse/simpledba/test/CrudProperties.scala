package io.doolse.simpledba.test

import cats.{Id, Monad, ~>}
import io.doolse.simpledba.WriteQueries
import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Gen, Prop, Properties}
import cats.syntax.all._

/**
  * Created by jolz on 16/06/16.
  */
class CrudProperties[F[_] : Monad, A: Arbitrary, K](run: F ~> Id, writes: WriteQueries[F, A],
                                                    _findAll: A => F[Iterable[A]], expected: Int, genUpdate: Gen[(A, A)]) extends Properties("CRUD ops") {

  val findAll = (a:A) => _findAll(a).map(_.count(a.==))

  implicit def runProp(fa: F[Prop]): Prop = run(fa)

  property("createReadDelete") = forAll { (a: A) =>
    for {
      _ <- writes.insert(a)
      count <- findAll(a)
      _ <- writes.delete(a)
      afterDel <- findAll(a)
    } yield {
      s"Expected to find $expected" |: count == expected &&
        ("0 after delete" |: afterDel == 0)
    }
  }

  property("update") = forAll(genUpdate) { case (a1, a2) =>
    for {
      _ <- writes.insert(a1)
      changed <- writes.update(a1, a2)
      countOrig <- findAll(a1)
      countNew <- findAll(a2)
    } yield {
      "Values are different" |: (a1 != a2) ==> {
        s"Original should be gone - $countOrig" |: countOrig == 0 &&
          ("New values" |: countNew == expected)
      } ||
        ("Values are same" |: (a1 == a2) ==> {
          "Changed flag" |: !changed &&
            ("Same amount after update" |: (countOrig == expected))
        })
    }
  }
}
