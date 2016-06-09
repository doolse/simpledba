package test

import cats.Monad
import cats.syntax.all._
import cats.std.option._
import io.doolse.simpledba._
import shapeless.HNil

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
                           instByPK: SingleQuery[F, Inst, Long],
                           querybyFirstName: MultiQuery[F, User, String],
                           queryByLastName: MultiQuery[F, User, String],
                           queryByFullName: SingleQuery[F, User, Username]
                          )

  val model = RelationModel(
    embed[EmbeddedFields],
    relation[Inst]('institution).key('uniqueid),
    relation[User]('users).keys('firstName, 'lastName)
  ).queries[Queries](
    writes('institution),
    writes('users),
    queryByPK('institution),
    query('users).multipleByColumns('firstName).sortBy('year),
    query('users).multipleByColumns('lastName),
    queryByPK('users)
  )

  val orig = Inst(1L, EmbeddedFields("pass", enabled = true))
  val updated = Inst(2L, EmbeddedFields("pass", enabled = false))
  val updatedAgain = Inst(2L, EmbeddedFields("changed", enabled = true))

  def doTest[F[_] : Monad](q: Queries[F]) = {
    import q._
    for {
      _ <- writeUsers.insert(User("Jolse", "Maginnis", 1980))
      _ <- writeUsers.insert(User("Emma", "Maginnis", 1982))
      _ <- writeUsers.insert(User("Jolse", "Fuckstick", 1985))
      _ <- writeInst.insert(orig)
      res2 <- instByPK.query(1L)
      res <- instByPK.query(517573426L)
      _ <- writeInst.update(orig, updated)
      res3 <- instByPK.query(2L)
      _ <- writeInst.update(updated, updatedAgain)
      res4 <- instByPK.query(2L)
      all <- queryByLastName.queryWithOrder("Maginnis", true)
      allFirst <- querybyFirstName.queryWithOrder("Jolse", false)
      fullPK <- queryByFullName.query(Username("Jolse", "Maginnis"))
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
