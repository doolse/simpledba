package io.doolse.simpledba.test

import java.util.UUID

import cats.Monad
import cats.effect.Sync
import cats.implicits._
import io.doolse.simpledba._
import io.doolse.simpledba.syntax._
import org.scalacheck.Arbitrary._
import org.scalacheck.Prop._
import org.scalacheck.Test.Parameters
import org.scalacheck.{Arbitrary, Prop, Shrink}
import fs2.Stream
import SortedQueryProperties._

/**
  * Created by jolz on 21/06/16.
  */


case class Sortable(pk1: UUID, same: UUID, intField: Int, stringField: SafeString, shortField: Short,
                    longField: Long, floatField: Float, doubleField: Double, uuidField: UUID)

object SortedQueryProperties
{
  case class Queries[F[_]](writes: WriteQueries[F, Sortable],
                           truncate: Stream[F, WriteOp],
                           int1: UUID => Stream[F, Sortable],
                           int2: UUID => Stream[F, Sortable],
                           string1: UUID => Stream[F, Sortable],
                           string2: UUID => Stream[F, Sortable],
                           short1: UUID => Stream[F, Sortable],
                           short2: UUID => Stream[F, Sortable],
                           long1: UUID => Stream[F, Sortable],
                           long2: UUID => Stream[F, Sortable],
                           float1: UUID => Stream[F, Sortable],
                           float2: UUID => Stream[F, Sortable],
                           double1: UUID => Stream[F, Sortable],
                           double2: UUID => Stream[F, Sortable],
                           uuid1: UUID => Stream[F, Sortable],
                           uuid2: UUID => Stream[F, Sortable]
                          )

}
abstract class SortedQueryProperties[F[_] : Monad : Sync](implicit arb: Arbitrary[Sortable]) extends AbstractRelationsProperties[F]("Sorting") {

  implicit def flushable: Flushable[F]

  val queries: (Queries[F], Queries[F])

  implicit val shrinkSortable = Shrink[Sortable](_ => scala.Stream.empty)

  case class OrderQuery[A](lens: Sortable => A,
                           query: Queries[F] => UUID => Stream[F, Sortable])(implicit val o: Ordering[A])

  def checkOrder[A](same: UUID, v: Vector[Sortable], sortQ: Seq[(String, OrderQuery[_])]) = {
    val vSame = v.map(_.copy(same = same))
    for {
      _ <- (queries._1.truncate ++ queries._1.writes.insertAll(Stream(vSame: _*))).flush.compile.drain
      p = sortQ.map {
        case (name, oq @ OrderQuery(lens, q)) => run(for {
          ascend <- q(queries._1)(same).map(lens).compile.toVector
          descend <- q(queries._2)(same).map(lens).compile.toVector
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
      "int only" -> OrderQuery(_.intField, _.int1),
      "int and String" -> OrderQuery(s => (s.intField, s.stringField), _.int2)
    ))
  }

  property("Sorted by string") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID, l, Seq(
      "string only" -> OrderQuery(_.stringField, _.string1),
      "string and short" -> OrderQuery(s => (s.stringField,s.shortField), _.string2)
    ))
  }

  property("Sorted by short") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID, l, Seq(
      "short only" -> OrderQuery(_.shortField, _.short1),
      "short and double" -> OrderQuery(s => (s.shortField, s.doubleField), _.short2)
    ))
  }

  property("Sorted by long") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID, l, Seq(
      "long only" -> OrderQuery(_.longField, _.long1),
      "long and float" -> OrderQuery(s => (s.longField, s.floatField), _.long2)
    ))
  }

  property("Sorted by float") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID, l, Seq(
      "float only" -> OrderQuery(_.floatField, _.float1),
      "float and uuid" -> OrderQuery(s => (s.floatField, s.uuidField), _.float2)
    ))
  }

  property("Sorted by double") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID, l, Seq(
      "double only" -> OrderQuery(_.doubleField, _.double1),
      "double and int" -> OrderQuery(s => (s.doubleField, s.intField), _.double2)
    ))
  }

  val uuidTextSort = new Ordering[UUID] {
    def compare(x: UUID, y: UUID): Int = x.toString.compareTo(y.toString)
  }

  property("Sorted by uuid") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID, l, Seq(
      "uuid only" -> OrderQuery(_.uuidField, _.uuid1)(uuidTextSort),
      "uuid and long" -> OrderQuery(s => (s.uuidField, s.longField), _.uuid2)(
        Ordering.Tuple2(uuidTextSort, implicitly[Ordering[Long]]))
    ))
  }

}
