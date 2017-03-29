package io.doolse.simpledba.test

import java.util.UUID

import cats.Monad
import cats.implicits._
import fs2.util.{Async, Catchable}
import io.doolse.simpledba._
import org.scalacheck.Arbitrary._
import org.scalacheck.Prop._
import org.scalacheck.Test.Parameters
import org.scalacheck.{Arbitrary, Prop, Shrink}
import fs2.Stream

/**
  * Created by jolz on 21/06/16.
  */
case class Sortable(pk1: UUID, same: UUID, intField: Int, stringField: SafeString, shortField: Short,
                    longField: Long, floatField: Float, doubleField: Double, uuidField: UUID)

abstract class SortedQueryProperties[F[_] : Monad : Catchable : Flushable](implicit arb: Arbitrary[Sortable]) extends AbstractRelationsProperties[F]("Sorting") {

  case class Queries[F[_]](writes: WriteQueries[F, Sortable],
                           int1: SortableQuery[F, Sortable, UUID],
                           int2: SortableQuery[F, Sortable, UUID],
                           string1: SortableQuery[F, Sortable, UUID],
                           string2: SortableQuery[F, Sortable, UUID],
                           short1: SortableQuery[F, Sortable, UUID],
                           short2: SortableQuery[F, Sortable, UUID],
                           long1: SortableQuery[F, Sortable, UUID],
                           long2: SortableQuery[F, Sortable, UUID],
                           float1: SortableQuery[F, Sortable, UUID],
                           float2: SortableQuery[F, Sortable, UUID],
                           double1: SortableQuery[F, Sortable, UUID],
                           double2: SortableQuery[F, Sortable, UUID],
                           uuid1: SortableQuery[F, Sortable, UUID],
                           uuid2: SortableQuery[F, Sortable, UUID]
                          )

  val sortableRel = relation[Sortable]('sortable).key('pk1)
  private val sameSortable = query(sortableRel).multipleByColumns('same)
  val model = RelationModel(sortableRel).queries[Queries](
    writes(sortableRel),
    sameSortable.sortBy('intField),
    sameSortable.sortBy('intField, 'stringField),
    sameSortable.sortBy('stringField),
    sameSortable.sortBy('stringField, 'shortField),
    sameSortable.sortBy('shortField),
    sameSortable.sortBy('shortField, 'doubleField),
    sameSortable.sortBy('longField),
    sameSortable.sortBy('longField, 'floatField),
    sameSortable.sortBy('floatField),
    sameSortable.sortBy('floatField, 'uuidField),
    sameSortable.sortBy('doubleField),
    sameSortable.sortBy('doubleField, 'intField),
    sameSortable.sortBy('uuidField),
    sameSortable.sortBy('uuidField, 'longField)
  )

  val queries: Queries[F]

  implicit val shrinkSortable = Shrink[Sortable](_ => scala.Stream.empty)

  case class OrderQuery[A](lens: Sortable => A, query: SortableQuery[F, Sortable, UUID])(implicit val o: Ordering[A])

  def checkOrder[A](same: UUID, v: Vector[Sortable], sortQ: Seq[(String, OrderQuery[_])]) = {
    val vSame = v.map(_.copy(same = same))
    for {
      _ <- (queries.writes.truncate >> queries.writes.bulkInsert(Stream(vSame: _*))).flush
      p = sortQ.map {
        case (name, oq @ OrderQuery(lens, q)) => run(for {
          ascend <- q.queryWithOrder(same, asc = true).map(lens).runLog
          descend <- q.queryWithOrder(same, asc = false).map(lens).runLog
        } yield {
          val ord = oq.o
          val ascExpected = vSame.map(lens).sorted(ord)
          val descExpected = vSame.map(lens).sorted(ord.reverse)
          s"Ascending - $name" |: (ascend ?= ascExpected) &&
            (s"Descending - $name" |: (descend ?= descExpected))
        })
      }
    } yield Prop.all(p: _*)
  }

  property("Sorted by int") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID, l, Seq(
      "int only" -> OrderQuery(_.intField, queries.int1),
      "int and String" -> OrderQuery(s => (s.intField, s.stringField), queries.int2)
    ))
  }

  property("Sorted by string") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID, l, Seq(
      "string only" -> OrderQuery(_.stringField, queries.string1),
      "string and short" -> OrderQuery(s => (s.stringField,s.shortField), queries.string2)
    ))
  }

  property("Sorted by short") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID, l, Seq(
      "short only" -> OrderQuery(_.shortField, queries.short1),
      "short and double" -> OrderQuery(s => (s.shortField, s.doubleField), queries.short2)
    ))
  }

  property("Sorted by long") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID, l, Seq(
      "long only" -> OrderQuery(_.longField, queries.long1),
      "long and float" -> OrderQuery(s => (s.longField, s.floatField), queries.long2)
    ))
  }

  property("Sorted by float") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID, l, Seq(
      "float only" -> OrderQuery(_.floatField, queries.float1),
      "float and uuid" -> OrderQuery(s => (s.floatField, s.uuidField), queries.float2)
    ))
  }

  property("Sorted by double") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID, l, Seq(
      "double only" -> OrderQuery(_.doubleField, queries.double1),
      "double and int" -> OrderQuery(s => (s.doubleField, s.intField), queries.double2)
    ))
  }

  implicit val uuidTextSort = new Ordering[UUID] {
    def compare(x: UUID, y: UUID): Int = x.toString.compareTo(y.toString)
  }

  property("Sorted by uuid") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID, l, Seq(
      "uuid only" -> OrderQuery(_.uuidField, queries.uuid1),
      "uuid and long" -> OrderQuery(s => (s.uuidField, s.longField), queries.uuid2)
    ))
  }

}
