package io.doolse.simpledba.test

import java.util.UUID

import cats.Monad
import cats.implicits._
import io.doolse.simpledba._
import io.doolse.simpledba.syntax._
import org.scalacheck.Arbitrary._
import org.scalacheck.Prop._
import org.scalacheck.Test.Parameters
import org.scalacheck.{Arbitrary, Prop, Shrink}

/**
  * Created by jolz on 21/06/16.
  */
case class Sortable(pk1: UUID,
                    same: UUID,
                    intField: Int,
                    stringField: SafeString,
                    shortField: Short,
                    longField: Long,
                    floatField: Float,
                    doubleField: Double,
                    uuidField: UUID)

abstract class SortedQueryProperties[S[_[_], _], F[_]: Monad](implicit arb: Arbitrary[Sortable])
    extends AbstractRelationsProperties[S, F]("Sorting") {

  case class Queries(writes: WriteQueries[S, F, Sortable],
                     truncate: S[F, WriteOp],
                     int1: UUID => S[F, Sortable],
                     int2: UUID => S[F, Sortable],
                     string1: UUID => S[F, Sortable],
                     string2: UUID => S[F, Sortable],
                     short1: UUID => S[F, Sortable],
                     short2: UUID => S[F, Sortable],
                     long1: UUID => S[F, Sortable],
                     long2: UUID => S[F, Sortable],
                     float1: UUID => S[F, Sortable],
                     float2: UUID => S[F, Sortable],
                     double1: UUID => S[F, Sortable],
                     double2: UUID => S[F, Sortable],
                     uuid1: UUID => S[F, Sortable],
                     uuid2: UUID => S[F, Sortable])

  implicit def SM = S.M
  val queries: (Queries, Queries)

  implicit val shrinkSortable = Shrink[Sortable](_ => scala.Stream.empty)

  case class OrderQuery[A](lens: Sortable => A, query: Queries => UUID => S[F, Sortable])(
      implicit val o: Ordering[A])

  def checkOrder[A](same: UUID, v: Vector[Sortable], sortQ: Seq[(String, OrderQuery[_])]) = {
    val vSame = v.map(_.copy(same = same))
    for {
      _ <- flushed(S.append(queries._1.truncate, queries._1.writes.insertAll(S.emits(vSame))))
      p = sortQ.map {
        case (name, oq @ OrderQuery(lens, q)) =>
          run(for {
            ascend  <- S.toVector(q(queries._1)(same).map(lens))
            descend <- S.toVector(q(queries._2)(same).map(lens))
          } yield {
            val ord          = oq.o
            val ascExpected  = vSame.map(lens).sorted(ord)
            val descExpected = vSame.map(lens).sorted(ord.reverse)
            s"Ascending - $name" |: (ascend ?= ascExpected) &&
            (s"Descending - $name" |: (descend ?= descExpected))
          })
      }
    } yield Prop.all(p: _*)
  }

  property("Sorted by int") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID,
               l,
               Seq(
                 "int only"       -> OrderQuery(_.intField, _.int1),
                 "int and String" -> OrderQuery(s => (s.intField, s.stringField), _.int2)
               ))
  }

  property("Sorted by string") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID,
               l,
               Seq(
                 "string only"      -> OrderQuery(_.stringField, _.string1),
                 "string and short" -> OrderQuery(s => (s.stringField, s.shortField), _.string2)
               ))
  }

  property("Sorted by short") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID,
               l,
               Seq(
                 "short only"       -> OrderQuery(_.shortField, _.short1),
                 "short and double" -> OrderQuery(s => (s.shortField, s.doubleField), _.short2)
               ))
  }

  property("Sorted by long") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID,
               l,
               Seq(
                 "long only"      -> OrderQuery(_.longField, _.long1),
                 "long and float" -> OrderQuery(s => (s.longField, s.floatField), _.long2)
               ))
  }

  property("Sorted by float") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID,
               l,
               Seq(
                 "float only"     -> OrderQuery(_.floatField, _.float1),
                 "float and uuid" -> OrderQuery(s => (s.floatField, s.uuidField), _.float2)
               ))
  }

  property("Sorted by double") = forAll { (l: Vector[Sortable]) =>
    checkOrder(UUID.randomUUID,
               l,
               Seq(
                 "double only"    -> OrderQuery(_.doubleField, _.double1),
                 "double and int" -> OrderQuery(s => (s.doubleField, s.intField), _.double2)
               ))
  }

  val uuidTextSort = new Ordering[UUID] {
    def compare(x: UUID, y: UUID): Int = x.toString.compareTo(y.toString)
  }

  property("Sorted by uuid") = forAll { (l: Vector[Sortable]) =>
    checkOrder(
      UUID.randomUUID,
      l,
      Seq(
        "uuid only" -> OrderQuery(_.uuidField, _.uuid1)(uuidTextSort),
        "uuid and long" -> OrderQuery(s => (s.uuidField, s.longField), _.uuid2)(
          Ordering.Tuple2(uuidTextSort, implicitly[Ordering[Long]]))
      )
    )
  }

}
