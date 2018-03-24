package io.doolse.simpledba.test

import cats.Monad
import cats.effect.Sync
import fs2.{Pipe, Stream}
import io.doolse.simpledba.test.Test.User
import io.doolse.simpledba2.{ReadQueries, WriteQueries}
import cats.instances.option._
import cats.syntax.all._

object Test {

  case class EmbeddedFields(adminpassword: String, enabled: Boolean)

  case class Inst(uniqueid: Long, embedded: EmbeddedFields)

  case class Username(fn: String, ln: String)

  case class User(firstName: String, lastName: String, year: Int)

  case class Queries[F[_]](writeInst: WriteQueries[F, Inst],
                           writeUsers: WriteQueries[F, User],
                           instByPK: ReadQueries[F, Long, Inst],
                           querybyFirstName: Boolean => ReadQueries[F, String, User],
                           queryByLastName: Boolean => ReadQueries[F, String, User],
                           queryByFullName: ReadQueries[F, Username, User]
                          )

  val orig = Inst(1L, EmbeddedFields("pass", enabled = true))
  val updated = Inst(2L, EmbeddedFields("pass", enabled = false))
  val updatedAgain = Inst(2L, EmbeddedFields("changed", enabled = true))

  def insertData[F[_]](writeInst: WriteQueries[F, Inst],
                       writeUsers: WriteQueries[F, User]) = {
    writeUsers.insertAll(Stream(User("Jolse", "Maginnis", 1980),
      User("Emma", "Maginnis", 1982),
      User("Jolse", "Mahinnis", 1985)
    )) ++ writeInst.insert(orig)
  }

  def doTest[F[_] : Monad : Sync](q: Queries[F]) = {
    import q._
    for {
      _ <- insertData(q.writeInst, q.writeUsers)
      res2 <- instByPK(1L)
      res <- instByPK(517573426L)
      _ <- writeInst.update(orig, updated)
      res3 <- instByPK(2L).last
      _ <- writeInst.update(updated, updatedAgain)
      res4 <- instByPK(2L).last
      all <- Stream.eval(queryByLastName(true)("Maginnis").compile.toVector)
      allFirst <- Stream.eval(querybyFirstName(false)("Jolse").compile.toVector)
      fullPK <- Stream.eval(queryByFullName(Username("Jolse", "Maginnis")).compile.toVector)
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