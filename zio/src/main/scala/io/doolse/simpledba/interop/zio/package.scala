package io.doolse.simpledba.interop

import java.util.concurrent.CompletableFuture

import _root_.zio.interop.catz._
import _root_.zio.interop.javaconcurrent._
import _root_.zio.stream._
import _root_.zio.{Task, TaskR, ZIO, RIO}
import _root_.zio.console._
import cats.Monad
import io.doolse.simpledba.{IOEffects, JavaEffects, StreamEffects, WriteQueries}

package object zio {

  type ZStreamR[-R, +A] = ZStream[R, Throwable, A]
  type ZIOWriteQueries[-R, W, T] = WriteQueries[ZStreamR, RIO, R, W, T]

  implicit def zioJavaEffect = new JavaEffects[RIO] {
    override def blockingIO[A](thunk: => A): RIO[Any, A] = RIO(thunk)

    override def fromFuture[A](future: () => CompletableFuture[A]): RIO[Any, A] =
      Task.fromCompletionStage(future)
  }

  val zioStreamEffects = new StreamEffects[ZStreamR, RIO] {

    override def eval[R, A](fa: ZIO[R, Throwable, A]): ZStream[R, Throwable, A] =
      ZStream.fromEffect(fa)

    override def evalMap[R, R1 <: R, A, B](sa: ZStream[R, Throwable, A])(
        f: A => ZIO[R1, Throwable, B]): ZStream[R1, Throwable, B] = sa.mapM(f)

    override def empty[R, A]: ZStream[R, Throwable, A] = ZStream.empty

    override def emit[A](a: A): ZStream[Any, Throwable, A] = ZStream.succeed(a)

    override def emits[A](a: Seq[A]): ZStream[Any, Throwable, A] = ZStream(a: _*)

    override def foldLeft[R, O, O2](s: ZStream[R, Throwable, O], z: O2)(
        f: (O2, O) => O2): ZStream[R, Throwable, O2] = ZStream.fromEffect(s.foldLeft(z)(f))

    override def append[R, R1 <: R, A](a: ZStream[R, Throwable, A],
                           b: ZStream[R1, Throwable, A]): ZStream[R1, Throwable, A] = a ++ b

    override def drain[R](s: ZStream[R, Throwable, _]): ZIO[R, Throwable, Unit] =
      s.run(ZSink.drain)

    override def bracket[R, A](acquire: ZIO[R, Throwable, A])(
        release: A => ZIO[R, Throwable, Unit]): ZStream[R, Throwable, A] =
      ZStream.bracket(acquire)(release.andThen(_.ignore))

    override def maxMapped[R, R1 <: R, A, B](n: Int, s: ZStream[R, Throwable, A])(f: Seq[A] => B): ZStream[R1, Throwable, B]
      = s.transduce(ZSink.foldUntil[List[A], A](Nil, n)((s,a) => a :: s)).map(l => f(l.reverse))

    override def read1[R, A](s: ZStream[R, Throwable, A]): ZIO[R, Throwable, A] =
      s.run(ZSink.read1[Throwable, A](_ => new Throwable("Expected one value"))(_ => true))

    override def flatMapS[R, R1 <: R, A, B](fa: ZStream[R, Throwable, A])(fb: A => ZStream[R1, Throwable, B]): ZStream[R1, Throwable, B] = fa.flatMap(fb)

    override def flatMapF[R, R1 <: R, A, B](fa: ZIO[R, Throwable, A])(fb: A => ZIO[R1, Throwable, B]): ZIO[R1, Throwable, B] = fa.flatMap(fb)

    override def productR[R, R1 <: R, A, B](l: ZIO[R, Throwable, A])(r: ZIO[R1, Throwable, B]): ZIO[R1, Throwable, B] = l *> r

    override def mapF[R, A, B](fa: ZIO[R, Throwable, A])(f: A => B): ZIO[R, Throwable, B] = fa.map(f)

    override def mapS[R, A, B](fa: ZStream[R, Throwable, A])(f: A => B): ZStream[R, Throwable, B] = fa.map(f)

    override val unit: ZIO[Any, Throwable, Unit] = ZIO.unit

    override def delay[A](a: => A): ZIO[Any, Throwable, A] = ZIO.effect(a)
  }

  implicit val taskREffects : IOEffects[RIO] = zioStreamEffects
}
