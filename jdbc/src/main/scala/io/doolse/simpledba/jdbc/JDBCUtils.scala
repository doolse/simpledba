package io.doolse.simpledba.jdbc

import java.sql.SQLType
import cats.syntax.traverse._
import cats.instances.vector._

import fs2.Task
import fs2.interop.cats._

/**
  * Created by jolz on 12/03/17.
  */
object JDBCUtils {

  def brackets(c: Iterable[String]): String = c.mkString("(", ",", ")")


  def createSchema(session: JDBCSession, tables: Iterable[JDBCCreateTable]): Task[Unit] = {
    tables.toVector.traverse(t => session.execWrite(t, Seq.empty)).map(_ => ())
  }


}
