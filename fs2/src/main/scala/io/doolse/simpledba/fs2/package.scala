package io.doolse.simpledba

import _root_.fs2.Stream
import cats.effect.{IO, Sync}

package object fs2 {
  type StreamIOR[-R, +A] = Stream[IO, A]
  type IOR[-R, A] = IO[A]

  private type StreamR[+F[-_, _], -R, +A] = Stream[F[R, ?], A]

  implicit val catsIOEffects : IOEffects[IOR] = fs2StreamEffects[IOR]

  def fs2StreamEffects[F[-_, _]](implicit _FS: Sync[F[Any, ?]]): StreamEffects[StreamR[F, -?, ?], F] =
    new StreamEffects[StreamR[F, -?, ?], F] {

      implicit def FS[R] : Sync[F[R, ?]] = _FS.asInstanceOf[Sync[F[R, ?]]]

      override def eval[R, A](fa: F[R, A]): StreamR[F, R, A] = Stream.eval(fa)

      override def empty[R, A]: StreamR[F, R, A] = Stream.empty

      override def emit[A](a: A): StreamR[F, Any, A] = Stream.emit(a).covary[F[Any, ?]]

      override def emits[A](a: Seq[A]): StreamR[F, Any, A] = Stream.emits(a).covary[F[Any, ?]]

      override def foldLeft[R, O, O2](s: StreamR[F, R, O], z: O2)(f: (O2, O) => O2): StreamR[F, R, O2] =
        s.fold(z)(f)

      override def append[R, R1 <: R, A](a: StreamR[F, R, A], b: StreamR[F, R1, A]): StreamR[F, R1, A] = a ++ b

      override def drain[R](s: StreamR[F, R, _]): F[R, Unit] = s.compile.drain

      override def read[R, A, B](acquire: F[R, A])(release: A => F[R, Unit])(
          read: A => F[R, Option[B]]): StreamR[F, R, B] = {
        val s = Stream.bracket(acquire)(release)
        def loop(a: A): StreamR[F, R, B] = {
          Stream.eval(read(a)).flatMap {
            case None    => Stream.empty
            case Some(b) => Stream.emit(b) ++ loop(a)
          }
        }
        s.flatMap(loop)
      }

      override def evalMap[R, R1 <: R, A, B](sa: StreamR[F, R, A])(f: A => F[R1, B]): StreamR[F, R1, B] = sa.evalMap(f)


      override def bracket[R, A](acquire: F[R, A])(release: A => F[R, Unit]): StreamR[F, R, A] =
        Stream.bracket(acquire)(release)

      override def maxMapped[R, R1 <: R, A, B](n: Int, s: StreamR[F, R, A])(f: Seq[A] => B): StreamR[F, R1, B] =
        s.chunkN(n).map(c => f(c.toVector))

      override def read1[R, A](s: StreamR[F, R, A]): F[R, A] = flatMapF(s.compile.toList)(
        _.headOption.fold[F[R, A]](FS.raiseError(new Throwable("Expected a single result")))(FS.pure[A]))

      override def flatMapS[R, R1 <: R, A, B](fa: StreamR[F, R, A])(fb: A => StreamR[F, R1, B]): StreamR[F, R1, B] = fa.flatMap(fb)

      override def flatMapF[R, R1 <: R, A, B](fa: F[R, A])(fb: A => F[R1, B]): F[R1, B] = FS[R1].flatMap(fa)(fb)

      override def productR[R, R1 <: R, A, B](l: F[R, A])(r: F[R1, B]): F[R1, B] = FS[R1].productR(l)(r)

      override def mapF[R, A, B](fa: F[R, A])(f: A => B): F[R, B] = FS.map(fa)(f)

      override def mapS[R, A, B](fa: StreamR[F, R, A])(f: A => B): StreamR[F, R, B] = fa.map(f)

      override val unit: F[Any, Unit] = FS.unit

      override def delay[A](a: => A): F[Any, A] = FS.delay(a)
    }
}
