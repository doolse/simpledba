package io.doolse.simpledba.test

import java.util.UUID

import cats.Monad
import cats.implicits._
import io.doolse.simpledba._
import io.doolse.simpledba.syntax._
import org.scalacheck.Arbitrary._
import org.scalacheck.Prop._
import org.scalacheck.Test.Parameters
import org.scalacheck.{Arbitrary, Gen, Prop, Shrink}

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
                    uuidField: UUID)

abstract class SortedQueryProperties[S[_], F[_]: Monad]
    extends AbstractRelationsProperties[S, F]("Sorting") {

  case class Queries(int1: UUID => S[Sortable],
                     int2: UUID => S[Sortable],
                     string1: UUID => S[Sortable],
                     string2: UUID => S[Sortable],
                     short1: UUID => S[Sortable],
                     short2: UUID => S[Sortable],
                     long1: UUID => S[Sortable],
                     long2: UUID => S[Sortable],
                     float1: UUID => S[Sortable],
                     float2: UUID => S[Sortable],
                     uuid1: UUID => S[Sortable],
                     uuid2: UUID => S[Sortable],
                     writes: WriteQueries[S, F, Sortable],
                     truncate: S[WriteOp],
                    )

  val queries: (Queries, Queries)

  val genSimple: Gen[Sortable] = for {
    id          <- arbitrary[UUID]
    intField    <- arbitrary[Int]
    shortField  <- arbitrary[Short]
    stringField <- Gen.alphaNumStr
    longField   <- arbitrary[Long]
    floatField  <- arbitrary[Float]
  } yield Sortable(id, id, intField, SafeString(stringField), shortField, longField, floatField, id)

  implicit val shrinkSortable = Shrink[Sortable](_ => scala.Stream.empty)

  case class OrderQuery[A](lens: Sortable => A, query: Queries => UUID => S[Sortable])(
      implicit val o: Ordering[A])

  def checkOrder[A](same: UUID, v: Seq[Sortable], sortQ: Seq[(String, OrderQuery[_])]) = {
    val vSame       = uniqueify[Sortable](v.map(_.copy(same = same)), _.pk1).toVector
    val S           = streamable
    implicit val SM = S.SM
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

  val genSortables = Gen.listOfN(10, genSimple)

  property("Sorted by int") = forAll(genSortables) { l: List[Sortable] =>
    checkOrder(UUID.randomUUID,
               l,
               Seq(
                 "int only"       -> OrderQuery(_.intField, _.int1),
                 "int and String" -> OrderQuery(s => (s.intField, s.stringField), _.int2)
               ))
  }

  property("Sorted by string") = forAll(genSortables) { l: List[Sortable] =>
    checkOrder(UUID.randomUUID,
               l,
               Seq(
                 "string only"      -> OrderQuery(_.stringField, _.string1),
                 "string and short" -> OrderQuery(s => (s.stringField, s.shortField), _.string2)
               ))
  }

  val uuidTextSort = new Ordering[UUID] {
    def compare(x: UUID, y: UUID): Int = x.toString.compareTo(y.toString)
  }

  property("Sorted by short") = forAll(genSortables) { l: List[Sortable] =>
    checkOrder(UUID.randomUUID,
               l,
               Seq(
                 "short only"     -> OrderQuery(_.shortField, _.short1),
                 "short and uuid" -> OrderQuery(s => (s.shortField, s.uuidField), _.short2)(
                   Ordering.Tuple2(implicitly[Ordering[Short]], uuidTextSort))
               )
               )
  }

  property("Sorted by long") = forAll(genSortables) { l: List[Sortable] =>
    checkOrder(UUID.randomUUID,
               l,
               Seq(
                 "long only"      -> OrderQuery(_.longField, _.long1),
                 "long and float" -> OrderQuery(s => (s.longField, s.floatField), _.long2)
               ))
  }

  property("Sorted by float") = forAll(genSortables) { l: List[Sortable] =>
    checkOrder(UUID.randomUUID,
               l,
               Seq(
                 "float only"     -> OrderQuery(_.floatField, _.float1),
                 "float and uuid" -> OrderQuery(s => (s.floatField, s.uuidField), _.float2)
               ))
  }

  property("Sorted by uuid") = forAll(genSortables) { l: List[Sortable] =>
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
