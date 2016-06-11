package io.doolse.simpledba.cassandra

import java.nio.ByteBuffer

import cats.Applicative
import cats.std.list._
import cats.syntax.traverse._
import com.datastax.driver.core._
import com.datastax.driver.core.schemabuilder.{Create, SchemaBuilder}
import fs2.interop.cats._
import fs2.util.Task
/**
  * Created by jolz on 9/06/16.
  */
object CassandraUtils {

  implicit def str2Statement(s: String) = new SimpleStatement(s)
  def whenM[F[_], A](b: Boolean, fa: => F[A])(implicit M: Applicative[F]): F[Unit] = if (b) M.map(fa)(_ => ()) else M.pure()

  def initKeyspaceAndSchema(session: SessionConfig, keyspace: String, creation: List[(String, Create)],
                            dropKeyspace: Boolean = false, dropTables: Boolean = false) : Task[Unit] = for {
    _ <- whenM(dropKeyspace, session.executeLater(s"DROP KEYSPACE IF EXISTS $keyspace"))
    _ <- session.executeLater(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};")
    _ <- session.executeLater(s"USE $keyspace")
    _ <- createSchema(session, creation, dropFirst = dropTables)
  } yield ()

  def createSchema(session: SessionConfig, creation: List[(String, Create)], dropFirst: Boolean = false) : Task[Unit] = {
    creation.traverse[Task, Unit] {
      case (name, c) => for {
        _ <- whenM(dropFirst, session.executeLater(SchemaBuilder.dropTable(name).ifExists()))
        _ <- session.executeLater(c)
      } yield ()
    }.map(_ => ())
  }
}
