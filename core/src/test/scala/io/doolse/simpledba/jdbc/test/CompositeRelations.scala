package io.doolse.simpledba.jdbc.test

import java.util.UUID

import cats.Monad
import cats.effect.Sync
import cats.syntax.all._
import io.doolse.simpledba._
import CompositeRelations._
import io.doolse.simpledba.{Flushable, ReadQueries, WriteQueries}
import org.scalacheck.Shapeless._

/**
  * Created by jolz on 16/06/16.
  */
object CompositeRelations {

  case class Composite2(pkLong: Long, pkUUID: UUID, fieldInt: Int, fieldString: String, fieldBool: Boolean)

  case class Composite3(pkInt: Int, pkString: String, pkBool: Boolean, fieldLong: Long, fieldUUID: UUID)

  case class Queries2[F[_]](updates: WriteQueries[F, Composite2], byPK: ReadQueries[F, (Long, UUID), Composite2])

  case class Queries3[F[_]](updates: WriteQueries[F, Composite3], byPK: ReadQueries[F, (Int, String, Boolean), Composite3])

}


abstract class CompositeRelations[F[_] : Sync](name: String)(implicit M: Monad[F], F:Flushable[F]) extends AbstractRelationsProperties[F](s"$name - Composite") {

  val queries2 : Queries2[F]

  val queries3 : Queries3[F]

  include(crudProps[Composite2, UUID](queries2.updates,
    a => queries2.byPK((a.pkLong, a.pkUUID)), 1,
    genUpdate[Composite2]((a, b) => b.copy(pkLong = a.pkLong, pkUUID = a.pkUUID))), "Composite2")

  include(crudProps[Composite3, UUID](queries3.updates,
    a => queries3.byPK((a.pkInt, a.pkString, a.pkBool)), 1,
    genUpdate[Composite3]((a, b) => b.copy(pkInt = a.pkInt, pkString = a.pkString, pkBool = a.pkBool))), "Composite3")

}