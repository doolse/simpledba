package io.doolse.simpledba.test

import java.util.UUID

import cats.Monad
import cats.syntax.all._
import io.doolse.simpledba._
import io.doolse.simpledba.test.CompositeRelations._
import org.scalacheck.Shapeless._

/**
  * Created by jolz on 16/06/16.
  */
object CompositeRelations {

  case class Composite2(pkLong: Long, pkUUID: UUID, fieldInt: Int, fieldString: String, fieldBool: Boolean)

  case class Composite3(pkInt: Int, pkString: String, pkBool: Boolean, fieldLong: Long, fieldUUID: UUID)

  case class Queries2[F[_]](updates: WriteQueries[F, Composite2], byPK: UniqueQuery[F, Composite2, (Long, UUID)])

  case class Queries3[F[_]](updates: WriteQueries[F, Composite3], byPK: UniqueQuery[F, Composite3, (Int, String, Boolean)])

  lazy val composite2Model = RelationModel(relation[Composite2]('composite2).keys('pkLong, 'pkUUID))
    .queries[Queries2](writes('composite2), queryByPK('composite2))

  lazy val composite3Model = RelationModel(relation[Composite3]('composite3).keys('pkInt, 'pkString, 'pkBool))
    .queries[Queries3](writes('composite3), queryByPK('composite3))

}


abstract class CompositeRelations[F[_]](name: String)(implicit M: Monad[F]) extends AbstractRelationsProperties[F](s"$name - Composite") {

  val queries2 : Queries2[F]

  val queries3 : Queries3[F]

  include(crudProps[Composite2, UUID](queries2.updates,
    a => queries2.byPK((a.pkLong, a.pkUUID)).map(_.toIterable), 1,
    genUpdate[Composite2]((a, b) => b.copy(pkLong = a.pkLong, pkUUID = a.pkUUID))), "Composite2")

  include(crudProps[Composite3, UUID](queries3.updates,
    a => queries3.byPK((a.pkInt, a.pkString, a.pkBool)).map(_.toIterable), 1,
    genUpdate[Composite3]((a, b) => b.copy(pkInt = a.pkInt, pkBool = a.pkBool))), "Composite3")

}