package test

import cats.Monad
import io.doolse.simpledba.cassandra._
import shapeless.HList

/**
  * Created by jolz on 5/05/16.
  */
object TestCassandra extends App {

  val cassdb = CassandraRelationIO()

  implicit def catsMonad[F[_]](implicit SM: scalaz.Monad[F]) = new Monad[F] {
    override def map[A, B](fa: F[A])(f: (A) => B): F[B] = SM.map(fa)(f)

    def flatMap[A, B](fa: F[A])(f: (A) => F[B]): F[B] = SM.bind(fa)(f)

    def pure[A](x: A): F[A] = SM.pure(x)
  }

  case class Inst(uniqueid: Long, adminpassword: String, enabled: Boolean)

  val mapper = new CassandraMapper()

  import mapper._

  val table = mapper.table[Inst]("institution").key('uniqueid)
  val pt = mapper.physicalTable(table)

  val init = CassandraRelationIO.initialiseSession(CassandraSession.simpleSession("localhost", Some("eps")))
  println(TestQuery.doQueryWithTable(mapper)(pt, HList(517573426L)) .run(init).unsafePerformSync)
}
