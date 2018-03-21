package io.doolse.simpledba2

import java.sql.DriverManager

import shapeless._
import syntax.singleton._
import fs2._
import io.doolse.simpledba2.Relation.DBIO

case class Embedded(blah: Int, destroy: Double)
case class Frogs(pk: String, embedded: Embedded)

object Tester extends App {

  val connection = DriverManager.getConnection("jdbc:postgresql:simpledba2", "equellauser", "tle010")

  implicit val cols = PostgresMapper.embedded(LabelledGeneric[Embedded])
  val simpleTable = PostgresMapper.table(LabelledGeneric[Frogs], "bogs", 'pk)

  val byPK = Query.byPK(simpleTable)

  println(byPK(Stream(HList("pk"), HList("pk2"))).compile.toVector.runA(connection).unsafeRunSync())


}
