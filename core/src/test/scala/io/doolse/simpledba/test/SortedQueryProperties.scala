package io.doolse.simpledba.test

import java.util.UUID

import cats.implicits._
import cats.Monad
import io.doolse.simpledba._
import org.scalacheck.Prop._
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Shapeless._

/**
  * Created by jolz on 21/06/16.
  */
abstract class SortedQueryProperties[F[_] : Monad](name: String) extends AbstractRelationsProperties[F](name+ " Sorting") {

  case class Sortable(pk1: UUID, same: UUID, intField: Int, stringField: String, shortField: Short, longField: Long, floatField: Float, doubleField: Double)

  case class Queries[F[_]](writes: WriteQueries[F, Sortable], int1: SortableQuery[F, Sortable, UUID])
  //, int2: SortableQuery[F, Sortable, String])

  val model = RelationModel(relation[Sortable]('sortable).key('pk1)).queries[Queries](
    writes('sortable),
    query('sortable).multipleByColumns('same).sortBy('intField),
    query('sortable).multipleByColumns('same).sortBy('intField, 'stringField)
  )

  def queries: Queries[F]

  def gen1(v: UUID) = for {
    sl <- arbitrary[Sortable]
  } yield sl.copy(same = v)

  val genList = {
    for {
      sz <- Gen.size
      u <- {
        val u = UUID.randomUUID()
        Gen.const(u)
      }
      l <- Gen.listOfN(sz, gen1(u))
    } yield {
      run(l.traverse(queries.writes.insert))
      (l, u)
    }
  }

  property("Sort by Int") = forAll(genList) {
    case (ls,u) => true
  }
}
