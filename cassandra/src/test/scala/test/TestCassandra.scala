package test

import cats.Monad
import com.datastax.driver.core.TypeCodec
import io.doolse.simpledba.cassandra.{CassandraCodecColumn, CassandraColumn, CassandraRelationIO, CassandraSession}

/**
  * Created by jolz on 5/05/16.
  */
object TestCassandra extends App {

  val longCol : CassandraColumn[Long] = CassandraCodecColumn(TypeCodec.bigint(), Long2long, _.asInstanceOf[AnyRef])
  val boolCol : CassandraColumn[Boolean] = CassandraCodecColumn(TypeCodec.cboolean(), Boolean2boolean, _.asInstanceOf[AnyRef])
  val stringCol  = CassandraCodecColumn.direct[String](TypeCodec.varchar())
  val cassdb = CassandraRelationIO()

  implicit def catsMonad[F[_]](implicit SM: scalaz.Monad[F]) = new Monad[F] {
    override def map[A, B](fa: F[A])(f: (A) => B): F[B] = SM.map(fa)(f)

    def flatMap[A, B](fa: F[A])(f: (A) => F[B]): F[B] = SM.bind(fa)(f)

    def pure[A](x: A): F[A] = SM.pure(x)
  }


  val init = CassandraRelationIO.initialiseSession(CassandraSession.simpleSession("localhost", Some("eps")))
  println(TestQuery.doQuery(cassdb)(boolCol, stringCol, longCol).run(init).unsafePerformSync)
}
