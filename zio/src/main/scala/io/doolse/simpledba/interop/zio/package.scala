package io.doolse.simpledba.interop

import java.util.concurrent.CompletableFuture

import _root_.zio.interop.catz._
import _root_.zio.interop.javaconcurrent._
import _root_.zio.stream._
import _root_.zio.{Task, TaskR, ZIO}
import cats.Monad
import io.doolse.simpledba.{JavaEffects, Streamable}

package object zio {

  implicit def zioJavaEffect[R] = new JavaEffects[TaskR[R, ?]] {
    override def blockingIO[A](thunk: => A): TaskR[R, A] = TaskR(thunk)

    override def fromFuture[A](future: () => CompletableFuture[A]): TaskR[R, A] =
      Task.fromCompletionStage(future)
  }

  implicit def zioStreamable = new Streamable[ZStream[Any, Throwable, ?], ZIO[Any, Throwable, ?]] {

    def onceOnly[A, R, E]: A => ZIO[R, E, Option[A]] = {
      var first = true
      a =>
        if (first) {
          first = false
          ZIO.succeed(Some(a))
        } else ZIO.succeed(None)
    }

    override def M: Monad[ZIO[Any, Throwable, ?]] = Monad[ZIO[Any, Throwable, ?]]

    override def SM: Monad[ZStream[Any, Throwable, ?]] = new Monad[ZStream[Any, Throwable, ?]] {
      override def flatMap[A, B](fa: ZStream[Any, Throwable, A])(
          f: A => ZStream[Any, Throwable, B]): ZStream[Any, Throwable, B] = fa.flatMap(f)

      override def pure[A](x: A): ZStream[Any, Throwable, A] = ZStream.succeed(x)

      override def tailRecM[A, B](a: A)(
          f: A => ZStream[Any, Throwable, Either[A, B]]): ZStream[Any, Throwable, B] = f(a).flatMap {
        case Left(a) => tailRecM(a)(f)
        case Right(b) => ZStream.succeed(b)
      }
    }

    override def eval[A](fa: ZIO[Any, Throwable, A]): ZStream[Any, Throwable, A] =
      ZStream.fromEffect(fa)

    override def evalMap[A, B](sa: ZStream[Any, Throwable, A])(
        f: A => ZIO[Any, Throwable, B]): ZStream[Any, Throwable, B] = sa.mapM(f)

    override def empty[A]: ZStream[Any, Throwable, A] = ZStream.empty

    override def emit[A](a: A): ZStream[Any, Throwable, A] = ZStream.succeed(a)

    override def emits[A](a: Seq[A]): ZStream[Any, Throwable, A] = ZStream(a: _*)

    override def foldLeft[O, O2](s: ZStream[Any, Throwable, O], z: O2)(
        f: (O2, O) => O2): ZStream[Any, Throwable, O2] = ZStream.fromEffect(s.foldLeft(z)(f).use(ZIO.succeed[O2]))

    override def append[A](a: ZStream[Any, Throwable, A],
                           b: ZStream[Any, Throwable, A]): ZStream[Any, Throwable, A] = a ++ b

    override def drain(s: ZStream[Any, Throwable, _]): ZIO[Any, Throwable, Unit] =
      s.run(ZSink.drain)

    override def bracket[A](acquire: ZIO[Any, Throwable, A])(
        release: A => ZIO[Any, Throwable, Unit]): ZStream[Any, Throwable, A] =
      ZStream.bracket(acquire)(release.andThen(_.orDie))

    override def maxMapped[A, B](n: Int, s: ZStream[Any, Throwable, A])(f: Seq[A] => B): ZStream[Any, Throwable, B]
      = s.transduce(ZSink.identity[A].collectAllN(n).mapError(_ => throw new Throwable("How?")) ).map(f)

    override def read1[A](s: ZStream[Any, Throwable, A]): ZIO[Any, Throwable, A] =
      s.run(ZSink.read1[Throwable, A](_ => new Throwable("Expected one value"))(_ => true))
  }
}
