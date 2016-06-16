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
import SimpleRelations._

/**
  * Created by jolz on 15/06/16.
  */

object SimpleRelations {

  case class Fields1(uuid: UUID)

  case class Fields2(uuid: UUID, name: String)

  case class Fields3(uuid: UUID, name: String, year: Int)

  case class Fields1Queries[F[_]](updates: WriteQueries[F, Fields1], byPK: UniqueQuery[F, Fields1, UUID])

  case class Fields2Queries[F[_]](updates: WriteQueries[F, Fields2], byPK: UniqueQuery[F, Fields2, UUID],
                                  byName: SortableQuery[F, Fields2, String])

  case class Fields3Queries[F[_]](updates: WriteQueries[F, Fields3], byPK: UniqueQuery[F, Fields3, UUID],
                                  byName: RangeQuery[F, Fields3, String, Int], byYear: SortableQuery[F, Fields3, Int])

  lazy val fields1Model = RelationModel(relation[Fields1]('fields1).key('uuid))
    .queries[Fields1Queries](writes('fields1), queryByPK('fields1))

  lazy val fields2Model = RelationModel(relation[Fields2]('fields2).key('uuid))
    .queries[Fields2Queries](writes('fields2), queryByPK('fields2), query('fields2).multipleByColumns('name))

  lazy val fields3Model = RelationModel(relation[Fields3]('fields3).key('uuid))
    .queries[Fields3Queries](writes('fields3), queryByPK('fields3), query('fields3).multipleByColumns('name).sortBy('year),
    query('fields3).multipleByColumns('year).sortBy('name))
}

abstract class SimpleRelations[F[_]](name: String)(implicit M: Monad[F])
  extends AbstractRelationsProperties[F](s"$name - Relation Shapes") {

  def queries1: Fields1Queries[F]

  def queries2: Fields2Queries[F]

  def queries3: Fields3Queries[F]

  include(crudProps[Fields1, UUID](queries1.updates,
    a => queries1.byPK(a.uuid).map(_.toIterable), 1,
    genUpdate[Fields1]((a, b) => b.copy(uuid = a.uuid))), "Fields1")

  include(crudProps[Fields2, UUID](queries2.updates, a => for {
      oA <- queries2.byPK(a.uuid)
      l <- queries2.byName(a.name)
    } yield oA ++ l,
    2, genUpdate[Fields2]((a, b) => b)), "Fields2")

  include(crudProps[Fields3, UUID](queries3.updates, a => for {
      oA <- queries3.byPK(a.uuid)
      l <- queries3.byName(a.name)
      l2 <- queries3.byYear(a.year)
    } yield oA ++ l ++ l2,
    3, genUpdate[Fields3]((a, b) => b)), "Fields3")

  property("sorted results") = forAllNoShrink { (l: Vector[Fields3]) =>
    val name = UUID.randomUUID().toString
    val sameName = l.map(_.copy(name = name))
    for {
      _ <- sameName.traverse(queries3.updates.insert)
      ascend <- queries3.byName.queryWithOrder(name, asc = true).map(_.map(_.year))
      descend <- queries3.byName.queryWithOrder(name, asc = false).map(_.map(_.year))
    } yield {
      val ascExpected = sameName.map(_.year).sorted
      val descExpected = sameName.map(_.year).sorted(Ordering.Int.reverse)
      "Ascending" |: (ascend ?= ascExpected) &&
        ("Descending" |: (descend ?= descExpected))
    }
  }

  implicit def validRange[A](implicit o: Ordering[A], ab: Arbitrary[RangeValue[A]]) = Arbitrary {
    for {
      r1 <- ab.arbitrary
      r2 <- ab.arbitrary
    } yield {
       val (lr, ur) = (r1.value, r2.value) match {
        case (Some(a), Some(b)) if o.gteq(a, b) => if (o.equiv(a, b)) (r1, NoRange) else (r2, r1)
        case _ => (r1, r2)
      }
      FilterRange(lr, ur)
    }
  }

  property("range results") = forAllNoShrink { (l: Vector[Fields3], range: FilterRange[Int]) =>
    val name = UUID.randomUUID().toString
    val sameName = l.map(_.copy(name = name))
    for {
      _ <- sameName.traverse(queries3.updates.insert)
      results <- queries3.byName(name, range.lower, range.upper)
    } yield {
      "Results are within range" |: !results.map(_.year).exists(v => !range.contains(v))
    }
  }
}
