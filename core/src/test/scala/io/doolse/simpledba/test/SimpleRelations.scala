package io.doolse.simpledba.test

import java.util.UUID

import cats.instances.vector._
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
import fs2.util.{Async, Catchable}
import org.scalacheck.Test.Parameters

import scala.concurrent.ExecutionContext

/**
  * Created by jolz on 15/06/16.
  */

object SimpleRelations {

  case class Fields1(uuid: UUID)

  case class Fields2(uuid: UUID, name: SafeString)

  case class Fields3(uuid: UUID, name: SafeString, year: Int)

  case class Fields1Queries[F[_]](updates: WriteQueries[F, Fields1], byPK: UniqueQuery[F, Fields1, UUID])

  case class Fields2Queries[F[_]](updates: WriteQueries[F, Fields2], byPK: UniqueQuery[F, Fields2, UUID],
                                  byName: SortableQuery[F, Fields2, SafeString])

  case class Fields3Queries[F[_]](updates: WriteQueries[F, Fields3], byPK: UniqueQuery[F, Fields3, UUID],
                                  byName: RangeQuery[F, Fields3, SafeString, Int], byYear: SortableQuery[F, Fields3, Int])

  lazy val fields1Model = {
    val field1Rel = relation[Fields1]('fields1).key('uuid)
    RelationModel(field1Rel).queries[Fields1Queries](writes(field1Rel), queryByPK(field1Rel))
  }

  lazy val fields2Model = {
    val field2Rel = relation[Fields2]('fields2).key('uuid)
    RelationModel(field2Rel)
      .queries[Fields2Queries](writes(field2Rel), queryByPK(field2Rel), query(field2Rel).multipleByColumns('name))
  }

  lazy val fields3Model = {
    val field3Rel = relation[Fields3]('fields3).key('uuid)
    RelationModel(field3Rel)
      .queries[Fields3Queries](writes(field3Rel), queryByPK(field3Rel), query(field3Rel).multipleByColumns('name).sortBy('year),
      query(field3Rel).multipleByColumns('year).sortBy('name))
  }
}

abstract class SimpleRelations[F[_] : Catchable](name: String)(implicit M: Monad[F])
  extends AbstractRelationsProperties[F](s"$name - Relation Shapes") {

  def queries1: Fields1Queries[F]

  def queries2: Fields2Queries[F]

  def queries3: Fields3Queries[F]

  include(crudProps[Fields1, UUID](queries1.updates,
    a => queries1.byPK(a.uuid), 1,
    genUpdate[Fields1]((a, b) => b.copy(uuid = a.uuid))), "Fields1")

  include(crudProps[Fields2, UUID](queries2.updates,
    a => queries2.byPK(a.uuid) ++ queries2.byName(a.name), 2,
    genUpdate[Fields2]((a, b) => b)), "Fields2")

  include(crudProps[Fields3, UUID](queries3.updates,
    a => queries3.byPK(a.uuid) ++ queries3.byName(a.name) ++ queries3.byYear(a.year), 3,
    genUpdate[Fields3]((a, b) => b)), "Fields3")
}
