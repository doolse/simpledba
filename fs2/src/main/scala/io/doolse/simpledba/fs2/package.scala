package io.doolse.simpledba

import _root_.fs2.Stream
import cats.effect.{IO, Sync}

package object fs2 {
  type StreamR[+F[_], -R, +A] = Stream[F, A]
  type StreamIOR[-R, +A] = Stream[IO, A]
  type IOR[-R, A] = IO[A]
  type FR[F[_], -R, A] = F[A]

  implicit val catsIOEffects : IOEffects[IOR] = ???

  def fs2Stream[F[_]](implicit FS: Sync[F]): StreamEffects[StreamR[F, -?, ?], FR[F, -?, ?]] =
    new StreamEffects[StreamR[F, -?, ?], FR[F, -?, ?]] {

      override def eval[R, A](fa: F[A]): Stream[F, A] = Stream.eval(fa)

      override def empty[R, A]: Stream[F, A] = Stream.empty

      override def emit[A](a: A): Stream[F, A] = Stream.emit(a).covary[F]

      override def emits[A](a: Seq[A]): Stream[F, A] = Stream.emits(a).covary[F]

      override def foldLeft[R, O, O2](s: Stream[F, O], z: O2)(f: (O2, O) => O2): Stream[F, O2] =
        s.fold(z)(f)

      override def append[R, R1, A](a: Stream[F, A], b: Stream[F, A]): Stream[F, A] = a ++ b

      override def drain[R](s: Stream[F, _]): F[Unit] = s.compile.drain

      override def read[R, A, B](acquire: F[A])(release: A => F[Unit])(
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

      override def evalMap[R, R1, A, B](sa: Stream[F, A])(f: A => F[B]): Stream[F, B] = sa.evalMap(f)


      override def bracket[R, A](acquire: F[A])(release: A => F[Unit]): Stream[F, A] =
        Stream.bracket(acquire)(release)

      override def maxMapped[R, A, B](n: Int, s: Stream[F, A])(f: Seq[A] => B): Stream[F, B] = s.chunkN(n).map(c => f(c.toVector))

      override def read1[R, A](s: Stream[F, A]): F[A] = flatMapF(s.compile.toList)(
        _.headOption.fold[F[A]](FS.raiseError(new Throwable("Expected a single result")))(FS.pure[A]))

      override def flatMapS[R, R1 <: R, A, B](fa: StreamR[F, R, A])(fb: A => StreamR[F, R1, B]): StreamR[F, R1, B] = fa.flatMap(fb)

      override def flatMapF[R, R1 <: R, A, B](fa: FR[F, R, A])(fb: A => FR[F, R1, B]): FR[F, R1, B] = FS.flatMap(fa)(fb)

      override def productR[R, R1 <: R, A, B](l: FR[F, R, A])(r: FR[F, R1, B]): FR[F, R1, B] = FS.productR(l)(r)

      override def mapF[R, A, B](fa: FR[F, R, A])(f: A => B): FR[F, R, B] = FS.map(fa)(f)

      override def mapS[R, A, B](fa: StreamR[F, R, A])(f: A => B): StreamR[F, R, B] = fa.map(f)

      override val unit: FR[F, Any, Unit] = FS.unit

      override def delay[A](a: => A): FR[F, Any, A] = FS.delay(a)
    }
}
