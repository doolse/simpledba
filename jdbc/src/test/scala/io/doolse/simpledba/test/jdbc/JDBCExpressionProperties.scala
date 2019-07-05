package io.doolse.simpledba.test.jdbc

import java.util.UUID

import io.doolse.simpledba.test.SimpleDBAProperties
import zio.Task
import zio.stream.{ZSink, ZStream}
import org.scalacheck._
import org.scalacheck.Prop._
import Arbitrary._
import io.doolse.simpledba.jdbc.BinOp
import io.doolse.simpledba.jdbc.BinOp.BinOp
import io.doolse.simpledba.test.zio.ZIOProperties

case class SimpleTable(id: UUID, value1: Int, value2: String)

object JDBCExpressionProperties
    extends SimpleDBAProperties("JDBC Expressions")
    with JDBCProperties[ZStream[Any, Throwable, ?], Task]
    with ZIOProperties {

  val simpleTable = mapper.mapped[SimpleTable].table("simpleTable").key('id)

  val writer = sqlQueries.writes(simpleTable)

  import sqlQueries.flush

  setup(simpleTable)

  val genSimple: Gen[SimpleTable] = for {
    id     <- arbitrary[UUID]
    value1 <- arbitrary[Int]
    value2 <- Gen.alphaNumStr
  } yield SimpleTable(id, value1, value2)

  val ops: Seq[BinOp] = Seq(BinOp.EQ, BinOp.GT, BinOp.GTE, BinOp.LT, BinOp.LTE)

  def filterOp[A](f: SimpleTable => A, op: BinOp, compare: A)(
      implicit o: Ordering[A]): SimpleTable => Boolean =
    t => {
      val opf = op match {
        case BinOp.EQ  => o.equiv _
        case BinOp.GT  => o.gt _
        case BinOp.GTE => o.gteq _
        case BinOp.LT  => o.lt _
        case BinOp.LTE => o.lteq _
      }
      opf(f(t), compare)
    }

  def insertData(rows: Seq[SimpleTable]): Task[Seq[SimpleTable]] = {
    val dupesRemoved = uniqueify[SimpleTable](rows, _.id)
    flush(streamable
        .emit(sqlQueries.sql(sqlQueries.dialect.truncateTable(simpleTable.definition))) ++ writer
        .insertAll(ZStream(dupesRemoved: _*)))
      .map(_ => dupesRemoved)
  }

  def compareResults(fromDB: Seq[SimpleTable], fromScala: Seq[SimpleTable]): Prop = {
    fromDB.sortBy(_.id).toList ?= fromScala.sortBy(_.id).toList
  }

  property("oneComparison") = forAll(Gen.listOfN(10, genSimple), arbitrary[Int], Gen.oneOf(ops)) {
    (rawRows, compareInt, op) =>
      run {
        for {
          rows <- insertData(rawRows)
          fromDB <- sqlQueries
            .query(simpleTable)
            .where('value1, op)
            .build[Int]
            .apply(compareInt)
            .runCollect
        } yield {
          compareResults(fromDB, rows.filter(filterOp(_.value1, op, compareInt))).label(op.toString)
        }
      }
  }

  property("twoComparisons") = forAll(Gen.listOfN(10, genSimple),
                                      arbitrary[Int],
                                      arbitrary[String],
                                      Gen.oneOf(ops),
                                      Gen.oneOf(ops)) {
    (rawRows, compareInt, compareString, op1, op2) =>
      run {
        for {
          rows <- insertData(rawRows)
          fromDB <- sqlQueries
            .query(simpleTable)
            .where('value1, op1)
            .where('value2, op2)
            .build[(Int, String)]
            .apply((compareInt, compareString))
            .runCollect
        } yield {
          compareResults(fromDB, rows.filter { r =>
            filterOp(_.value1, op1, compareInt).apply(r) &&
            filterOp(_.value2, op2, compareString).apply(r)
          }).label(s"${op1.toString} and ${op2.toString}")
        }
      }
  }

  property("deleteOneComparison") =
    forAll(Gen.listOfN(10, genSimple), arbitrary[Int], Gen.oneOf(ops)) {
      (rawRows, compareInt, op) =>
        run {
          for {
            rows <- insertData(rawRows)
            _ <- flush(
                sqlQueries
                  .deleteFrom(simpleTable)
                  .where('value1, op)
                  .build[Int]
                  .apply(compareInt))
            fromDB <- sqlQueries.allRows(simpleTable).runCollect
          } yield {
            compareResults(fromDB, rows.filterNot(filterOp(_.value1, op, compareInt)))
              .label(op.toString)
          }
        }
    }

  property("deleteTwoComparisons") = forAll(Gen.listOfN(10, genSimple),
                                            arbitrary[Int],
                                            arbitrary[String],
                                            Gen.oneOf(ops),
                                            Gen.oneOf(ops)) {
    (rawRows, compareInt, compareString, op1, op2) =>
      run {
        for {
          rows <- insertData(rawRows)
          _ <- flush(
              sqlQueries
                .deleteFrom(simpleTable)
                .where('value1, op1)
                .where('value2, op2)
                .build[(Int, String)]
                .apply((compareInt, compareString)))
          fromDB <- sqlQueries.allRows(simpleTable).runCollect
        } yield {
          compareResults(fromDB, rows.filterNot { r =>
            filterOp(_.value1, op1, compareInt).apply(r) &&
            filterOp(_.value2, op2, compareString).apply(r)
          }).label(s"${op1.toString} and ${op2.toString}")
        }
      }
  }

  property("whereIn") = forAll(Gen.choose(0, 500).flatMap(sz => Gen.listOfN(sz, genSimple))) {
    rawRows =>
      run {
        for {
          rows <- insertData(uniqueify[SimpleTable](rawRows, _.value1))
          fromDB <- sqlQueries
            .query(simpleTable)
            .whereIn('value1)
            .build[ZStream[Any, Throwable, Int]]
            .apply(ZStream(rows.map(_.value1): _*))
            .runCollect
        } yield {
          compareResults(fromDB, rows).label("IN")
        }
      }
  }

  property("whereIn and op") = forAll(Gen.choose(0, 500).flatMap(sz => Gen.listOfN(sz, genSimple)),
                                      Gen.oneOf(ops),
                                      arbitrary[Int]) { (rawRows, op, compareInt) =>
    run {
      for {
        rows <- insertData(uniqueify[SimpleTable](rawRows, _.value2))
        fromDB <- sqlQueries
          .query(simpleTable)
          .where('value1, op)
          .whereIn('value2)
          .build[(Int, ZStream[Any, Throwable, String])]
          .apply((compareInt, ZStream(rows.map(_.value2): _*)))
          .runCollect
      } yield {
        compareResults(fromDB, rows.filter(filterOp(_.value1, op, compareInt))).label("IN + op")
      }
    }
  }

}
