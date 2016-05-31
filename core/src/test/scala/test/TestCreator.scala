package test

import cats.Monad
import cats.data.ReaderT
import cats.syntax.all._
import io.doolse.simpledba.RelationModel._
import io.doolse.simpledba._
import shapeless._
import shapeless.syntax.singleton._
import cats.std.option._

/**
  * Created by jolz on 26/05/16.
  */

case class EmbeddedFields(adminpassword: String, enabled: Boolean)


case class Inst(uniqueid: Long, embedded: EmbeddedFields)

case class Username(fn: String, ln: String)

case class User(firstName: String, lastName: String, year: Int)


object TestCreator {

  case class Queries[F[_]](instByPK: SingleQuery[F, Inst, Long],
                           writeInst: WriteQueries[F, Inst],
                           querybyFirstName: MultiQuery[F, User, String],
                           writeUsers: WriteQueries[F, User],
                           queryByLastName: MultiQuery[F, User, String],
                           queryByFullName: SingleQuery[F, User, Username]
                          )

  val model = RelationModel(
    HList(embed[EmbeddedFields]),
    HList(
      'institution ->> relation[Inst]('uniqueid),
      'users ->> relation[User]('firstName, 'lastName)
    ),
    HList(
      queryFullKey('institution),
      queryWrites('institution),
      queryPartialKey('users, 'firstName),
      queryWrites('users),
      queryPartialKey('users, 'lastName),
      queryFullKey('users)
    )
  )

  val orig = Inst(1L, EmbeddedFields("pass", enabled = true))
  val updated = Inst(2L, EmbeddedFields("pass", enabled = false))
  val updatedAgain = Inst(2L, EmbeddedFields("changed", enabled = true))

  def doTest[F[_] : Monad](q:Queries[F]) = {
    import q._
    for {
      _ <- writeUsers.insert(User("Jolse", "Maginnis", 1980))
      _ <- writeUsers.insert(User("Emma", "Maginnis", 1982))
      _ <- writeUsers.insert(User("Jolse", "Fuckstick", 1980))
      _ <- writeInst.insert(orig)
      res2 <- instByPK.query(1L)
      res <- instByPK.query(517573426L)
      upd1 <- writeInst.update(orig, updated)
      res3 <- instByPK.query(2L)
      upd2 <- writeInst.update(updated, updatedAgain)
      res4 <- instByPK.query(2L)
      all <- queryByLastName.queryWithOrder("Maginnis", true)
      allFirst <- queryByFullName.query(Username("Jolse", "Maginnis"))
      _ <-  res4.map(writeInst.delete).sequence
    } yield (res2, res, upd1, res3, upd2, res4, all, allFirst)
  }
}
