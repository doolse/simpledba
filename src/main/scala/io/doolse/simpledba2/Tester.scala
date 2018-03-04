package io.doolse.simpledba2

import java.sql.DriverManager

import shapeless._
import syntax.singleton._
import fs2._

case class Frogs(pk: String, blah: Int, destroy: Double)

object Tester extends App {

  val connection = DriverManager.getConnection("jdbc:postgresql:simpledba2", "equellauser", "tle010")
  val gen = LabelledGeneric[Frogs]

  val simpleTable = PostgresMapper.table("bogs", gen, 'pk)

  val byPK = Query.byPK(simpleTable)

  println(byPK(Stream("pk", "pk2")).compile.toVector.runA(connection).unsafeRunSync())


}
