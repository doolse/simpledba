package io.doolse.simpledba.cassandra

import cats.Applicative
import cats.effect.IO
import cats.instances.vector._
import cats.syntax.traverse._
import com.datastax.driver.core._
import com.datastax.driver.core.schemabuilder.{Create, SchemaBuilder}
import io.doolse.simpledba.CatsUtils._
/**
  * Created by jolz on 9/06/16.
  */
object CassandraUtils {

  implicit def str2Statement(s: String) = new SimpleStatement(s)

  def initKeyspaceAndSchema(session: CassandraSession, keyspace: String, creation: Iterable[(String, Create)],
                            dropKeyspace: Boolean = false, dropTables: Boolean = false) : IO[Unit] = for {
    _ <- whenM(dropKeyspace, session.executeLater(s"DROP KEYSPACE IF EXISTS $keyspace"))
    _ <- session.executeLater(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};")
    _ <- session.executeLater(s"USE $keyspace")
    _ <- createSchema(session, creation, dropFirst = dropTables)
  } yield ()

  def createSchema(session: CassandraSession, creation: Iterable[(String, Create)], dropFirst: Boolean = false) : IO[Unit] = {
    creation.toVector.traverse[IO, Unit] {
      case (name, c) => for {
        _ <- whenM(dropFirst, session.executeLater(SchemaBuilder.dropTable(name).ifExists()))
        _ <- session.executeLater(c)
      } yield ()
    }.map(_ => ())
  }
}
