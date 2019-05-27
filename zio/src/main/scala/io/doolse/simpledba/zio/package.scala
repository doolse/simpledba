package io.doolse.simpledba

import java.util.concurrent.CompletableFuture

import cats.Monad
import scalaz.zio.interop.javaconcurrent._
import scalaz.zio.{Task, TaskR, ZIO, ZManaged}
import scalaz.zio.stream._
import scalaz.zio.interop.catz._

package object zio {

  implicit def zioJavaEffect[R] = new JavaEffects[TaskR[R, ?]] {
    override def blockingIO[A](thunk: => A): TaskR[R, A] = TaskR(thunk)

    override def fromFuture[A](future: () => CompletableFuture[A]): TaskR[R, A] =
      Task.fromCompletionStage(future)
  }

  implicit def zioStreamable = new Streamable[ZStream[Any, Throwable, ?], ZIO[Any, Throwable, ?]] {

    def onceOnly[A, R, E] : A => ZIO[R, E, Option[A]] = {
      var first = true
      a => if (first) {
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
          f: A => ZStream[Any, Throwable, Either[A, B]]): ZStream[Any, Throwable, B] = ???
    }

    override def eval[A](fa: ZIO[Any, Throwable, A]): ZStream[Any, Throwable, A] =
      ZStream.fromEffect(fa)

    override def evalMap[A, B](sa: ZStream[Any, Throwable, A])(
        f: A => ZIO[Any, Throwable, B]): ZStream[Any, Throwable, B] = sa.mapM(f)

    override def empty[A]: ZStream[Any, Throwable, A] = ZStream.empty

    override def emit[A](a: A): ZStream[Any, Throwable, A] = ZStream.succeed(a)

    override def emits[A](a: Seq[A]): ZStream[Any, Throwable, A] = ZStream(a: _*)

    override def scan[O, O2](s: ZStream[Any, Throwable, O], z: O2)(
        f: (O2, O) => O2): ZStream[Any, Throwable, O2] =  ZStream.fromEffect(s.foldLeft(z)(f))

    override def append[A](a: ZStream[Any, Throwable, A],
                           b: ZStream[Any, Throwable, A]): ZStream[Any, Throwable, A] = a ++ b

    override def read[A, B](acquire: ZIO[Any, Throwable, A],
        release: A => ZIO[Any, Throwable, Unit], read: A => ZIO[Any, Throwable, Option[B]]): ZStream[Any, Throwable, B] =
      ZStream.bracket(acquire)(a => release(a).orDie)(read)

    override def toVector[A](s: ZStream[Any, Throwable, A]): ZIO[Any, Throwable, Vector[A]] =
      s.run(ZSink.collect[A]).map(_.toVector)

    override def last[A](s: ZStream[Any, Throwable, A]): ZStream[Any, Throwable, Option[A]] =
      ZStream.fromEffect(s.run(ZSink.identity[A].?))

    override def drain(s: ZStream[Any, Throwable, _]): ZIO[Any, Throwable, Unit] =
      s.run(ZSink.drain)

    override def bracket[A](acquire: ZIO[Any, Throwable, A])(release: A => ZIO[Any, Throwable, Unit]): ZStream[Any, Throwable, A] =
      ZStream.bracket(acquire)(release.andThen(_.orDie))(onceOnly)
  }
}
