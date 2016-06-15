package io.doolse.simpledba.test

import java.util.UUID

import cats.std.vector._
import cats.{Id, Monad, ~>}
import io.doolse.simpledba._
import org.scalacheck._
import org.scalacheck.Prop._
import org.scalacheck.Arbitrary._
import org.scalacheck.Shapeless._
import cats.syntax.all._
import shapeless.Generic
import RelationShapes._

/**
  * Created by jolz on 15/06/16.
  */

class NonEmptyString(in: String) {
  val s = if (in.isEmpty) " " else in

  override def equals(a: Any) = a == s

  override def hashCode = s.hashCode
}

object RelationShapes {

  case class Fields1(uuid: UUID)

  case class Fields2(uuid: UUID, name: NonEmptyString)

  case class Fields3(uuid: UUID, name: NonEmptyString, year: Int)

  case class Fields1Queries[F[_]](updates: WriteQueries[F, Fields1], byPK: UniqueQuery[F, Fields1, UUID])

  case class Fields2Queries[F[_]](updates: WriteQueries[F, Fields2], byPK: UniqueQuery[F, Fields2, UUID], byName: SortableQuery[F, Fields2, NonEmptyString])

  case class Fields3Queries[F[_]](updates: WriteQueries[F, Fields3], byPK: UniqueQuery[F, Fields3, UUID],
                                  byName: RangeQuery[F, Fields3, NonEmptyString, Int], byYear: SortableQuery[F, Fields3, Int])

  lazy val fields1Model = RelationModel(relation[Fields1]('fields1).key('uuid))
    .queries[Fields1Queries](writes('fields1), queryByPK('fields1))

  lazy val fields2Model = RelationModel(atom(Generic[NonEmptyString]), relation[Fields2]('fields2).key('uuid))
    .queries[Fields2Queries](writes('fields2), queryByPK('fields2), query('fields2).multipleByColumns('name))

  lazy val fields3Model = RelationModel(atom(Generic[NonEmptyString]), relation[Fields3]('fields3).key('uuid))
    .queries[Fields3Queries](writes('fields3), queryByPK('fields3), query('fields3).multipleByColumns('name).sortBy('year),
    query('fields3).multipleByColumns('year).sortBy('name))
}

abstract class RelationShapes[F[_]](name: String)(implicit M: Monad[F]) extends Properties(s"$name - Relation Shapes") {
  implicit val arbUUID = Arbitrary(Gen.uuid)

  implicit def runProp(fa: F[Prop]): Prop = run(fa)


  class CrudOps[A: Arbitrary, K](writes: WriteQueries[F, A], findAll: A => F[Int], expected: Int, genUpdate: Gen[(A, A)]) extends Properties("CRUD ops") {
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

  def run[A](fa: F[A]): A

  def gen1: Fields1Queries[F]

  def gen2: Fields2Queries[F]

  def gen3: Fields3Queries[F]

  def genUpdate[A: Arbitrary](copyKey: (A, A) => A) = for {
    a <- arbitrary[A]
    b <- arbitrary[A]
    t <- Gen.frequency(75 -> true, 25 -> false)
  } yield {
    (a, if (t) copyKey(a, b) else b)
  }

  def count[A](a: A, fa: F[Iterable[A]]) = {
    fa.map(_.iterator.count(a.==))
  }

  include(new CrudOps[Fields1, UUID](gen1.updates,
    a => count(a, gen1.byPK(a.uuid).map(_.toIterable)), 1,
    genUpdate[Fields1]((a, b) => b.copy(uuid = a.uuid))), "Fields1")

  include(new CrudOps[Fields2, UUID](gen2.updates,
    a => count(a, for {
      oA <- gen2.byPK(a.uuid)
      l <- gen2.byName(a.name)
    } yield oA ++ l), 2,
    genUpdate[Fields2]((a, b) => b)), "Fields2")

  include(new CrudOps[Fields3, UUID](gen3.updates,
    a => count(a, for {
      oA <- gen3.byPK(a.uuid)
      l <- gen3.byName(a.name)
      l2 <- gen3.byYear(a.year)
    } yield oA ++ l ++ l2), 3,
    genUpdate[Fields3]((a, b) => b)), "Fields3")

  property("sorted results") = forAllNoShrink { (l: Vector[Fields3]) =>
    val name = new NonEmptyString(UUID.randomUUID().toString)
    val sameName = l.map(_.copy(name = name))
    for {
      _ <- sameName.traverse(gen3.updates.insert)
      ascend <- gen3.byName.queryWithOrder(name, asc = true).map(_.map(_.year))
      descend <- gen3.byName.queryWithOrder(name, asc = false).map(_.map(_.year))
    } yield {
      val ascExpected = sameName.sortWith((a,b) => a.year < b.year).map(_.year)
      val descExpected = sameName.sortWith((a,b) => a.year > b.year).map(_.year)
      "Ascending" |: (ascend ?= ascExpected) &&
        ("Descending" |: (descend ?= descExpected))
    }
  }
}
