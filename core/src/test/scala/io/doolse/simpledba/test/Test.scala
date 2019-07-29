package io.doolse.simpledba.test

import cats.Monad
import cats.instances.option._
import cats.syntax.all._
import io.doolse.simpledba.{StreamEffects, WriteQueries}

trait Test[SR[-_, _], FR[-_, _], W] {
  type S[A] = SR[Any, A]
  type F[A] = FR[Any, A]
  type Writes[A] = WriteQueries[SR, FR, Any, W, A]
  def streamable: StreamEffects[SR, FR]
  def last[A](s: S[A]): F[Option[A]]
  def toVector[A](s: S[A]): F[Vector[A]]
  def flush(s: S[W]): F[Unit]
  def SM : Monad[S]
  def M : Monad[F]

  case class EmbeddedFields(adminpassword: String, enabled: Boolean)

  case class Inst(uniqueid: Long, embedded: EmbeddedFields)

  case class Username(fn: String, ln: String)

  case class User(firstName: String, lastName: String, year: Int)

  case class Queries(initDB: F[Unit],
                     writeInst: Writes[Inst],
                     writeUsers: Writes[User],
                     insertNewInst: (Long => Inst) => F[Inst],
                     instByPK: Long => S[Inst],
                     querybyFirstNameAsc: String => S[User],
                     queryByLastNameDesc: String => S[User],
                     queryByFullName: Username => S[User],
                     justYear: Username => S[Int],
                     usersWithLastName: String => S[Int])

  def insertData(writeInst: Writes[Inst], writeUsers: Writes[User]) = {
    writeUsers.insertAll(
      streamable.emits(
        Seq(
          User("Jolse", "Maginnis", 1980),
          User("Emma", "Maginnis", 1982),
          User("Jolse", "Mahinnis", 1985)
        ))
    )
  }

  def doTest(q: Queries, updateId: (Inst, Inst) => Inst = (o, n) => n) : F[String] = {
    import q._
    implicit val _M = M
    implicit val _S = SM
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
      _               <- flush(res4.map(writeInst.delete).sequence.flatMap(wo => streamable.emits(wo.toSeq)))
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
