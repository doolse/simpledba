package io.doolse.simpledba.test

import java.sql.DriverManager

import io.doolse.simpledba.test.Test._
import io.doolse.simpledba2.jdbc._

object Tester extends App {

  val connection = DriverManager.getConnection("jdbc:postgresql:simpledba2", "equellauser", "tle010")

  implicit val config = PostgresMapper.postGresConfig
  implicit val cols = TableMapper[EmbeddedFields].embedded
  val instTable = TableMapper[Inst].table("inst").primaryKey('uniqueid)
  val userTable = TableMapper[User].table("user").primaryKeys('firstName, 'lastName)


  val inserted =
    JDBCQueries.flush(Test.insertData(JDBCQueries.writes(instTable), JDBCQueries.writes(userTable)))

  inserted.compile.drain.run(connection).unsafeRunSync()


}
