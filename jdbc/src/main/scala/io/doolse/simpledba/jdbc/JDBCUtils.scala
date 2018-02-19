package io.doolse.simpledba.jdbc

import java.sql.SQLType

import cats.effect.IO
import cats.syntax.traverse._
import cats.instances.vector._
import cats.syntax.apply._


/**
  * Created by jolz on 12/03/17.
  */
object JDBCUtils {

  def brackets(c: Iterable[String]): String = c.mkString("(", ",", ")")


  def createSchema(session: JDBCSession, tables: Iterable[JDBCCreateTable], drop: Boolean): IO[Unit] = {
    tables.toVector.traverse { t =>
      (if (drop) session.execWrite(JDBCDropTable(t.name), Seq.empty) else IO.pure(false)) *> session.execWrite(t, Seq.empty)
    }.map(_ => ())
  }


}
