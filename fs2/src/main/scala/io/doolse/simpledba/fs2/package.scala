package io.doolse.simpledba

import _root_.fs2.Stream
import cats.Monad
import cats.effect.Sync

package object fs2 {

  implicit def fs2Stream[F[_]: Sync]: Streamable[Stream[F, ?], F] =
    new Streamable[Stream[F, ?], F] {

      override def M: Monad[F] = implicitly[Sync[F]]

      override def SM: Monad[Stream[F, ?]] = implicitly[Monad[Stream[F, ?]]]

      override def eval[A](fa: F[A]): Stream[F, A] = Stream.eval(fa)

      override def empty[A]: Stream[F, A] = Stream.empty

      override def emit[A](a: A): Stream[F, A] = Stream.emit(a)

      override def emits[A](a: Seq[A]): Stream[F, A] = Stream.emits(a)

      override def scan[O, O2](s: Stream[F, O], z: O2)(f: (O2, O) => O2): Stream[F, O2] =
        s.scan(z)(f)

      override def append[A](a: Stream[F, A], b: Stream[F, A]): Stream[F, A] = a ++ b

      override def toVector[A](s: Stream[F, A]): F[Vector[A]] = s.compile.toVector

      override def drain(s: Stream[F, _]): F[Unit] = s.compile.drain

      override def read[A, B](acquire: F[A])(release: A => F[Unit])(
          read: A => F[Option[B]]): Stream[F, B] = {
        val s = Stream.bracket(acquire)(release)
        def loop(a: A): Stream[F, B] = {
          Stream.eval(read(a)).flatMap {
            case None    => Stream.empty
            case Some(b) => Stream.emit(b) ++ loop(a)
          }
        }
        s.flatMap(loop)
      }

      override def evalMap[A, B](sa: Stream[F, A])(f: A => F[B]): Stream[F, B] = sa.evalMap(f)

      override def last[A](s: Stream[F, A]): Stream[F, Option[A]] = s.last

      override def bracket[A](acquire: F[A])(release: A => F[Unit]): Stream[F, A] =
        Stream.bracket(acquire)(release)
    }
}
