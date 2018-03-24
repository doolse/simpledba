package io.doolse.simpledba2

import java.sql.DriverManager

import shapeless._
import syntax.singleton._
import fs2._
import io.doolse.simpledba2.jdbc._
import io.doolse.simpledba2.jdbc.Relation.DBIO

case class Embedded(blah: Int, destroy: Double)
case class Frogs(pk: String, embedded: Embedded)

object Tester extends App {

  val connection = DriverManager.getConnection("jdbc:postgresql:simpledba2", "equellauser", "tle010")

  implicit val config = PostgresMapper.postGresConfig
  implicit val cols = TableMapper[Embedded].embedded
  val simpleTable = TableMapper[Frogs].table("bogs", 'pk)

  val byPK = Query.byPK(simpleTable)

  println(byPK(Stream(HList("pk"), HList("pk2"))).compile.toVector.runA(connection).unsafeRunSync())


}
