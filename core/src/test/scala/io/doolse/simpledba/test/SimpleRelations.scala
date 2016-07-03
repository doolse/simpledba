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
import fs2._
import org.scalacheck.Test.Parameters

import scala.concurrent.ExecutionContext

/**
  * Created by jolz on 15/06/16.
  */

object SimpleRelations {

  case class Fields1(uuid: UUID)

  case class Fields2(uuid: UUID, name: NonEmptyString)

  case class Fields3(uuid: UUID, name: NonEmptyString, year: Int)

  case class Fields1Queries[F[_]](updates: WriteQueries[F, Fields1], byPK: UniqueQuery[F, Fields1, UUID])

  case class Fields2Queries[F[_]](updates: WriteQueries[F, Fields2], byPK: UniqueQuery[F, Fields2, UUID],
                                  byName: SortableQuery[F, Fields2, NonEmptyString])

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

abstract class SimpleRelations[F[_] : Async](name: String)(implicit M: Monad[F])
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
}
