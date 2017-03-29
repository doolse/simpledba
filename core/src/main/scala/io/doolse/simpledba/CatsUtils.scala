package io.doolse.simpledba

import cats.data.WriterT
import cats.{Applicative, Monad, Monoid, MonoidK}
import fs2.Stream
import fs2.util.{Attempt, Catchable}

/**
  * Created by jolz on 29/06/16.
  */
object CatsUtils {

  def whenM[F[_], A](b: Boolean, fa: => F[A])(implicit M: Applicative[F]): F[Unit] = if (b) M.map(fa)(_ => ()) else M.pure()


  def streamMonoid[F[_], A] = new Monoid[Stream[F, A]] {

    def empty: fs2.Stream[F,A] = Stream.empty
    def combine(x: fs2.Stream[F,A],y: fs2.Stream[F,A]): fs2.Stream[F,A] = x ++ y
  }

  implicit def writerTCatchableInstance[F[_], L](implicit F: Catchable[F], M:Monad[F], MN: Monoid[L]): Catchable[WriterT[F, L, ?]] = new Catchable[WriterT[F, L, ?]] {
    def pure[A](a: A): WriterT[F, L, A] = WriterT.value[F, L, A](a)
    override def map[A, B](fa: WriterT[F, L, A])(f: A => B): WriterT[F, L, B] = fa.map(f)
    def flatMap[A, B](fa: WriterT[F, L, A])(f: A => WriterT[F, L, B]): WriterT[F, L, B] = fa.flatMap(f)
    def attempt[A](fa: WriterT[F, L, A]): WriterT[F, L, Attempt[A]] = WriterT {
      M.map(F.attempt(fa.run))(aa => aa.fold(t => (MN.empty, Left(t)), la => (la._1, Right(la._2))))
    }
    def fail[A](t: Throwable): WriterT[F, L, A] = WriterT.lift(F.fail(t))
  }

}
