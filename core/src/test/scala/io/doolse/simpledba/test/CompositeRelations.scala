package io.doolse.simpledba.test

import java.util.UUID

import cats.Monad
import cats.syntax.all._
import io.doolse.simpledba._
import CompositeRelations._
import io.doolse.simpledba.WriteQueries
import org.scalacheck.Shapeless._

/**
  * Created by jolz on 16/06/16.
  */
object CompositeRelations {

  case class Composite2(pkLong: Long,
                        pkUUID: UUID,
                        fieldInt: Int,
                        fieldString: String,
                        fieldBool: Boolean)

  case class Composite3(pkInt: Int,
                        pkString: String,
                        pkBool: Boolean,
                        fieldLong: Long,
                        fieldUUID: UUID)

  case class Queries2[S[_[_], _], F[_]](updates: WriteQueries[S, F, Composite2],
                            byPK: ((Long, UUID)) => S[F, Composite2],
                            truncate: S[F, WriteOp])

  case class Queries3[S[_[_], _], F[_]](updates: WriteQueries[S, F, Composite3],
                            byPK: ((Int, String, Boolean)) => S[F, Composite3],
                            truncate: S[F, WriteOp])

}

abstract class CompositeRelations[S[_[_], _], F[_]](name: String)(implicit M: Monad[F])
    extends AbstractRelationsProperties[S, F](s"$name - Composite") {

  val queries2: Queries2[S, F]

  val queries3: Queries3[S, F]

  include(
    crudProps[Composite2, UUID](
      queries2.updates,
      queries2.truncate,
      a => queries2.byPK((a.pkLong, a.pkUUID)),
      1,
      genUpdate[Composite2]((a, b) => b.copy(pkLong = a.pkLong, pkUUID = a.pkUUID))),
    "Composite2"
  )

  include(
    crudProps[Composite3, UUID](
      queries3.updates,
      queries3.truncate,
      a => queries3.byPK((a.pkInt, a.pkString, a.pkBool)),
      1,
      genUpdate[Composite3]((a, b) =>
        b.copy(pkInt = a.pkInt, pkString = a.pkString, pkBool = a.pkBool))
    ),
    "Composite3"
  )

}
