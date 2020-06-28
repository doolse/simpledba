package io.doolse.simpledba.interop

import java.util.concurrent.CompletableFuture

import _root_.zio.interop.javaz._
import _root_.zio.stream._
import _root_.zio.{RIO, UIO, ZIO}
import io.doolse.simpledba.{JavaEffects, Streamable, WriteQueries}

package object zio {

  type StreamTask[A] = Stream[Throwable, A]
  type ZIOWriteQueries[R, W, T] = WriteQueries[ZStream[R, Throwable, *], RIO[R, *], W, T]

  implicit def zioJavaEffect[R] = new JavaEffects[RIO[R, *]] {
    override def blockingIO[A](thunk: => A): RIO[R, A] = RIO(thunk)

    override def fromFuture[A](future: => CompletableFuture[A]): RIO[R, A] =
      fromCompletionStage(UIO(future))
  }

  implicit def zioStreamEffects[R]: Streamable[ZStream[R, Throwable, *], RIO[R, *]] = new Streamable[ZStream[R, Throwable, *], RIO[R, *]] {

    override def eval[A](fa: ZIO[R, Throwable, A]): ZStream[R, Throwable, A] =
      ZStream.fromEffect(fa)

    override def evalMap[A, B](sa: ZStream[R, Throwable, A])(
      f: A => RIO[R, B]): ZStream[R, Throwable, B] = sa.mapM(f)

    override def empty[A]: ZStream[R, Throwable, A] = ZStream.empty

    override def emit[A](a: A): ZStream[Any, Throwable, A] = ZStream.succeed(a)

    override def emits[A](a: Seq[A]): ZStream[Any, Throwable, A] = ZStream(a: _*)

    override def foldLeft[O, O2](s: ZStream[R, Throwable, O], z: O2)(
      f: (O2, O) => O2): ZStream[R, Throwable, O2] = ZStream.fromEffect(s.fold(z)(f))

    override def append[A](a: ZStream[R, Throwable, A],
                           b: ZStream[R, Throwable, A]): ZStream[R, Throwable, A] = a ++ b

    override def drain(s: ZStream[R, Throwable, _]): ZIO[R, Throwable, Unit] =
      s.run(ZSink.drain)

    override def bracket[A](acquire: ZIO[R, Throwable, A])(
      release: A => ZIO[R, Throwable, Unit]): ZStream[R, Throwable, A] =
      ZStream.bracket(acquire)(release.andThen(_.ignore))

    override def maxMapped[A, B](n: Int, s: ZStream[R, Throwable, A])(f: Seq[A] => B): ZStream[R, Throwable, B]
    = s.transduce(ZTransducer.collectAllN(n)).map(f)

    override def read1[A](s: ZStream[R, Throwable, A]): ZIO[R, Throwable, A] =
      s.runHead.flatMap(o => ZIO.fromOption(o).mapError(_ => new Error("Expected one value")))

    override def flatMapS[A, B](fa: ZStream[R, Throwable, A])(fb: A => ZStream[R, Throwable, B]): ZStream[R, Throwable, B] = fa.flatMap(fb)

    override def mapS[A, B](fa: ZStream[R, Throwable, A])(f: A => B): ZStream[R, Throwable, B] = fa.map(f)
  }
}
