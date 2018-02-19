package io.doolse.simpledba.test

import java.util.UUID

import cats.Monad
import cats.effect.Sync
import cats.implicits._
import fs2.Stream
import io.doolse.simpledba._
import org.scalacheck.{Arbitrary, Prop, Shrink}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop._
import org.scalacheck.Shapeless._
import org.scalacheck.Test.Parameters

/**
  * Created by jolz on 21/06/16.
  */

case class DistinctRangeable(vals: Vector[Rangeable])
case class Rangeable(pk1: UUID, same: UUID, intField: Int, stringField: SafeString, shortField: Short,
                     longField: Long, floatField: Float, doubleField: Double, uuidField: UUID)

abstract class RangeQueryProperties[F[_] : Monad : Sync : Flushable](implicit rangeable: Arbitrary[Rangeable],
                                                          arbRange: Arbitrary[FilterRange[Rangeable]])
  extends AbstractRelationsProperties[F]("Ranges") {

  implicit val arbDistinctRangeable = for {
    vals <- arbitrary[Vector[Rangeable]]
  } yield DistinctRangeable(vals.groupBy(_.pk1).values.flatten.toVector)

  case class Queries[F[_]](writes: WriteQueries[F, Rangeable],
                           int1: RangeQuery[F, Rangeable, UUID, Int],
                           int2: RangeQuery[F, Rangeable, UUID, (Int, SafeString)],
                           string1: RangeQuery[F, Rangeable, UUID, SafeString],
                           string2: RangeQuery[F, Rangeable, UUID, (SafeString, Short)],
                           short1: RangeQuery[F, Rangeable, UUID, Short],
                           short2: RangeQuery[F, Rangeable, UUID, (Short, Double)],
                           long1: RangeQuery[F, Rangeable, UUID, Long],
                           long2: RangeQuery[F, Rangeable, UUID, (Long, Float)],
                           float1: RangeQuery[F, Rangeable, UUID, Float],
                           float2: RangeQuery[F, Rangeable, UUID, (Float, UUID)],
                           double1: RangeQuery[F, Rangeable, UUID, Double],
                           double2: RangeQuery[F, Rangeable, UUID, (Double, Int)],
                           uuid1: RangeQuery[F, Rangeable, UUID, UUID],
                           uuid2: RangeQuery[F, Rangeable, UUID, (UUID, Long)]
                          )

  val rangeableRel = relation[Rangeable]('rangeable).key('pk1)
  private val sameSortable = query(rangeableRel).multipleByColumns('same)
  val model = RelationModel(rangeableRel).queries[Queries](
    writes(rangeableRel),
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

  implicit val shrinkRange = Shrink[Rangeable](_ => scala.Stream.empty)
  implicit val shrinkRangeVal = Shrink[FilterRange[Rangeable]](_ => scala.Stream.empty)

  case class RangeCheck[A](lens: Rangeable => A, query: RangeQuery[F, Rangeable, UUID, A])(implicit val o: Ordering[A])

  def checkRange[A](same: UUID, dr: DistinctRangeable, _range: FilterRange[Rangeable], rangeQ: Seq[(String, RangeCheck[_])]) = {
    val v= dr.vals
    val vSame = v.map(_.copy(same = same))

    def processRangeCheck[A](name: String, oq: RangeCheck[A]) = {
      val ord = oq.o
      val q = oq.query
      val lens = oq.lens
      val (r1, r2) = (_range.lower.map(lens), _range.upper.map(lens))
      val range = (r1.value, r2.value) match {
        case (Some(a), Some(b)) if ord.gteq(a, b) => if (ord.equiv(a, b)) FilterRange(r1, NoRange) else FilterRange(r2, r1)
        case _ => FilterRange(r1, r2)
      }
      run(for {
        ascend <- q.queryWithOrder(same, range.lower, range.upper, asc = true).map(lens).compile.toVector
        descend <- q.queryWithOrder(same, range.lower, range.upper, asc = false).map(lens).compile.toVector
      } yield {
        val filtered = vSame.map(lens).filter(r => range.contains(r)(ord))
        val ascExpected = filtered.sorted(ord)
        val descExpected = filtered.sorted(ord.reverse)
        s"Range: ${range}" |: all(
          s"Ascending - $name" |: (ascend ?= ascExpected),
          s"Descending - $name" |: (descend ?= descExpected)
        )
      })
    }

    for {
      _ <- (queries.writes.truncate >> queries.writes.bulkInsert(Stream.emits(vSame)))
      p = rangeQ.map {
        case (name, oq) => processRangeCheck(name, oq)
      }
    } yield Prop.all(p: _*)
  }

  property("Range by int") = forAll { (l: DistinctRangeable, range: FilterRange[Rangeable]) =>
    checkRange(UUID.randomUUID, l, range, Seq(
      "int only" -> RangeCheck(_.intField, queries.int1)
//      "int and String" -> RangeCheck(s => (s.intField, s.stringField), queries.int2)
    ))
  }

  property("Range by string") = forAll { (l: DistinctRangeable, range: FilterRange[Rangeable]) =>
    checkRange(UUID.randomUUID, l, range, Seq(
      "string only" -> RangeCheck(_.stringField, queries.string1)
//      "string and short" -> RangeCheck(s => (s.stringField, s.shortField), queries.string2)
    ))
  }

  property("Range by short") = forAll { (l: DistinctRangeable, range: FilterRange[Rangeable]) =>
    checkRange(UUID.randomUUID, l, range, Seq(
      "short only" -> RangeCheck(_.shortField, queries.short1)
//      "short and double" -> RangeCheck(s => (s.shortField, s.doubleField), queries.short2)
    ))
  }

  property("Range by long") = forAll { (l: DistinctRangeable, range: FilterRange[Rangeable]) =>
    checkRange(UUID.randomUUID, l, range, Seq(
      "long only" -> RangeCheck(_.longField, queries.long1)
//      "long and float" -> RangeCheck(s => (s.longField, s.floatField), queries.long2)
    ))
  }

  property("Range by float") = forAll { (l: DistinctRangeable, range: FilterRange[Rangeable]) =>
    checkRange(UUID.randomUUID, l, range, Seq(
      "float only" -> RangeCheck(_.floatField, queries.float1)
//      "float and uuid" -> RangeCheck(s => (s.floatField, s.uuidField), queries.float2)
    ))
  }

  property("Range by double") = forAll { (l: DistinctRangeable, range: FilterRange[Rangeable]) =>
    checkRange(UUID.randomUUID, l, range, Seq(
      "double only" -> RangeCheck(_.doubleField, queries.double1)
//      "double and int" -> RangeCheck(s => (s.doubleField, s.intField), queries.double2)
    ))
  }

  implicit val uuidTextSort = new Ordering[UUID] {
    def compare(x: UUID, y: UUID): Int = x.toString.compareTo(y.toString)
  }

  property("Range by uuid") = forAll { (l: DistinctRangeable, range: FilterRange[Rangeable]) =>
    checkRange(UUID.randomUUID, l, range, Seq(
      "uuid only" -> RangeCheck(_.uuidField, queries.uuid1)
      //"uuid and long" -> RangeCheck(s => (s.uuidField, s.longField), queries.uuid2)
    ))
  }

}
