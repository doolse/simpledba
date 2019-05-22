package io.doolse.simpledba.test

import cats.instances.option._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import io.doolse.simpledba.{Flushable, Streamable, WriteOp, WriteQueries}

trait Test[S[_[_], _], F[_]] {

  def S: Streamable[S, F]
  def flusher: Flushable[S, F]

  def flush(s: S[F, WriteOp]) = flusher.flush(s)
  implicit def SM             = S.M

  case class EmbeddedFields(adminpassword: String, enabled: Boolean)

  case class Inst(uniqueid: Long, embedded: EmbeddedFields)

  case class Username(fn: String, ln: String)

  case class User(firstName: String, lastName: String, year: Int)

  case class Queries(initDB: F[Unit],
                     writeInst: WriteQueries[S, F, Inst],
                     writeUsers: WriteQueries[S, F, User],
                     insertNewInst: (Long => Inst) => S[F, Inst],
                     instByPK: Long => S[F, Inst],
                     querybyFirstNameAsc: String => S[F, User],
                     queryByLastNameDesc: String => S[F, User],
                     queryByFullName: Username => S[F, User],
                     justYear: Username => S[F, Int],
                     usersWithLastName: String => S[F, Int])

  def insertData(writeInst: WriteQueries[S, F, Inst], writeUsers: WriteQueries[S, F, User]) = {
    writeUsers.insertAll(
      S.emits(
        Seq(
          User("Jolse", "Maginnis", 1980),
          User("Emma", "Maginnis", 1982),
          User("Jolse", "Mahinnis", 1985)
        ))
    )
  }

  def last[A](s: S[F, A]): S[F, Option[A]] = S.last(s)
  def toVector[A](s: S[F, A]): S[F, Vector[A]] = S.eval(S.toVector(s))

  def doTest(q: Queries, updateId: (Inst, Inst) => Inst = (o, n) => n) = {
    import q._
    for {
      _    <- flush(insertData(q.writeInst, q.writeUsers))
      orig <- q.insertNewInst(id => Inst(id, EmbeddedFields("pass", enabled = true)))
      updated      = updateId(orig, Inst(2L, EmbeddedFields("pass", enabled = false)))
      updatedAgain = updateId(orig, Inst(2L, EmbeddedFields("changed", enabled = true)))
      res2            <- last(instByPK(orig.uniqueid))
      res             <- last(instByPK(517573426L))
      _               <- flush(writeInst.update(orig, updated))
      res3            <- last(instByPK(2L))
      _               <- flush(writeInst.update(updated, updatedAgain))
      res4            <- last(instByPK(2L))
      all             <- toVector(queryByLastNameDesc("Maginnis"))
      allFirst        <- toVector(querybyFirstNameAsc("Jolse"))
      fullPK          <- toVector(queryByFullName(Username("Jolse", "Maginnis")))
      _               <- flush(res4.map(writeInst.delete).sequence.flatMap(wo => S.emits(wo.toSeq)))
      yearOnly        <- last(q.justYear(Username("Jolse", "Maginnis")))
      countOfMaginnis <- last(q.usersWithLastName("Maginnis"))
    } yield {
      s"""
        |Query for known PK 1 - $res2
        |Query for missing key - $res
        |Query for update1 $res3
        |Query for update2 $res4
        |Query by last name ordered by first name ascending -
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
