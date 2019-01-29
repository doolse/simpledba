package io.doolse.simpledba.jdbc.test

import cats.Monad
import cats.effect.Sync
import cats.instances.option._
import cats.syntax.all._
import fs2.Stream
import io.doolse.simpledba.jdbc.JDBCIO
import io.doolse.simpledba.syntax._
import io.doolse.simpledba.{Flushable, WriteQueries}
import shapeless.{DepFn0, DepFn1, HList, HNil, SingletonProductArgs, Witness}
import shapeless.syntax.singleton._
import shapeless._
import shapeless.tag.@@
import scala.language.experimental.macros

object Test {

  case class EmbeddedFields(adminpassword: String, enabled: Boolean)

  case class Inst(uniqueid: Long, embedded: EmbeddedFields)

  case class Username(fn: String, ln: String)

  case class User(firstName: String, lastName: String, year: Int)

  case class Queries[F[_]](writeInst: WriteQueries[F, Inst],
                           writeUsers: WriteQueries[F, User],
                           insertNewInst: (Long => Inst) => Stream[F, Inst],
                           instByPK: Long => Stream[F, Inst],
                           querybyFirstNameAsc: String => Stream[F, User],
                           queryByLastNameDesc: String => Stream[F, User],
                           queryByFullName: Username => Stream[F, User],
                           justYear: Username => Stream[F, Int],
                           usersWithLastName: String => Stream[F, Int]
                          )

  val updated = Inst(2L, EmbeddedFields("pass", enabled = false))
  val updatedAgain = Inst(2L, EmbeddedFields("changed", enabled = true))

  def insertData[F[_]](writeInst: WriteQueries[F, Inst],
                       writeUsers: WriteQueries[F, User]) = {
    writeUsers.insertAll(
      Stream(
        User("Jolse", "Maginnis", 1980),
        User("Emma", "Maginnis", 1982),
        User("Jolse", "Mahinnis", 1985)
      )
    )
  }

  def doTest[F[_] : Monad : Sync : Flushable](q: Queries[F]) = {
    import q._
    for {
      _ <- insertData(q.writeInst, q.writeUsers).flush
      orig <- q.insertNewInst(id => Inst(id, EmbeddedFields("pass", enabled = true)))
      res2 <- instByPK(orig.uniqueid).last
      res <- instByPK(517573426L).last
      _ <- writeInst.update(orig, updated).flush
      res3 <- instByPK(2L).last
      _ <- writeInst.update(updated, updatedAgain).flush
      res4 <- instByPK(2L).last
      all <- Stream.eval(queryByLastNameDesc("Maginnis").compile.toVector)
      allFirst <- Stream.eval(querybyFirstNameAsc("Jolse").compile.toVector)
      fullPK <- Stream.eval(queryByFullName(Username("Jolse", "Maginnis")).compile.toVector)
      _ <- res4.map(writeInst.delete).sequence
      yearOnly <- q.justYear(Username("Jolse", "Maginnis")).last
      countOfMaginnis <- q.usersWithLastName("Maginnis").last
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
        |Year only - $yearOnly
        |Count of maginnis - $countOfMaginnis
      """.stripMargin
    }
  }
}
