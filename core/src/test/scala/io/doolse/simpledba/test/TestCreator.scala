package io.doolse.simpledba.test

import cats.Monad
import cats.syntax.all._
import io.doolse.simpledba._
import cats.std.list._
import cats.std.option._
import fs2.util.Catchable

/**
  * Created by jolz on 26/05/16.
  */
case class EmbeddedFields(adminpassword: String, enabled: Boolean)

case class Inst(uniqueid: Long, embedded: EmbeddedFields)

case class Username(fn: String, ln: String)

case class User(firstName: String, lastName: String, year: Int)


object TestCreator {

  case class Queries[F[_]](writeInst: WriteQueries[F, Inst],
                           writeUsers: WriteQueries[F, User],
                           instByPK: UniqueQuery[F, Inst, Long],
                           querybyFirstName: SortableQuery[F, User, String],
                           queryByLastName: SortableQuery[F, User, String],
                           queryByFullName: UniqueQuery[F, User, Username]
                          )

  val relInst = relation[Inst]('institution).key('uniqueid)
  val relUser = relation[User]('users).keys('firstName, 'lastName)
  val model = RelationModel(
    embed[EmbeddedFields],
    relInst,
    relUser
  ).queries[Queries](
    writes(relInst),
    writes(relUser),
    queryByPK(relInst),
    query(relUser).multipleByColumns('firstName).sortBy('year),
    query(relUser).multipleByColumns('lastName),
    queryByPK(relUser)
  )

  val orig = Inst(1L, EmbeddedFields("pass", enabled = true))
  val updated = Inst(2L, EmbeddedFields("pass", enabled = false))
  val updatedAgain = Inst(2L, EmbeddedFields("changed", enabled = true))

  def doTest[F[_] : Monad : Catchable](q: Queries[F]) = {
    import q._
    for {
      _ <- writeUsers.insert(User("Jolse", "Maginnis", 1980))
      _ <- writeUsers.insert(User("Emma", "Maginnis", 1982))
      _ <- writeUsers.insert(User("Jolse", "Mahinnis", 1985))
      _ <- writeInst.insert(orig)
      res2 <- instByPK(1L).runLast
      res <- instByPK.query(517573426L).runLast
      _ <- writeInst.update(orig, updated)
      res3 <- instByPK.query(2L).runLast
      _ <- writeInst.update(updated, updatedAgain)
      res4 <- instByPK.query(2L).runLast
      all <- queryByLastName.queryWithOrder("Maginnis", true).runLog
      allFirst <- querybyFirstName.queryWithOrder("Jolse", false).runLog
      fullPK <- queryByFullName.query(Username("Jolse", "Maginnis")).runLast
      _ <- res4.map(writeInst.delete).sequence
    } yield {
      s"""
        |Query for known PK 1 - $res2
        |Query for missing key - $res
        |Query for update1 $res3
        |Query for update2 $res4
        |Query by last name ordered arbitrarily -
        |${all.mkString(" ", "\n ", "")}
        |Query by first name ordered by year descending -
        |${allFirst.mkString(" ", "\n ", "")}
        |Query by case class pk - $fullPK
      """.stripMargin
    }
  }
}
