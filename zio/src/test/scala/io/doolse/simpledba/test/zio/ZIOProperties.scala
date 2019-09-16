package io.doolse.simpledba.test.zio

import cats.Monad
import cats.effect.Sync
import io.doolse.simpledba.interop.zio._
import io.doolse.simpledba.{JavaEffects, StreamEffects}
import zio.interop.catz._
import zio.stream._
import zio.{DefaultRuntime, RIO, Task, ZIO}

trait ZIOProperties {
  type SR[-R, A] = ZStream[R, Throwable, A]
  def streamable : StreamEffects[SR, RIO] = zioStreamEffects
  def M = implicitly[Monad[RIO[Any, ?]]]
  def SM = new Monad[ZStream[Any, Throwable, ?]] {
    override def pure[A](x: A): ZStream[Any, Throwable, A] = ZStream.succeed(x)

    override def flatMap[A, B](fa: ZStream[Any, Throwable, A])(f: A => ZStream[Any, Throwable, B]): ZStream[Any, Throwable, B] = fa.flatMap(f)

    override def tailRecM[A, B](a: A)(f: A => ZStream[Any, Throwable, Either[A, B]]): ZStream[Any, Throwable, B] = ???
  }
  def javaEffects : JavaEffects[RIO] = implicitly[JavaEffects[RIO]]
  def Sync : Sync[Task] = implicitly[Sync[Task]]
  def attempt[A](f: Task[A]) : Task[Either[Throwable, A]] = f.fold(Left.apply, Right.apply)
  lazy val runtime = new DefaultRuntime {}
  def run[A](prog: Task[A]): A = runtime.unsafeRun(prog)

  def toVector[A](s: ZStream[Any, Throwable, A]): ZIO[Any, Throwable, Vector[A]] =
    s.run(ZSink.collectAll[A]).map(_.toVector)
  def last[A](s: ZStream[Any, Throwable, A]): ZIO[Any, Throwable, Option[A]] =
    s.run(ZSink.collectAllN[A](1).map(_.headOption))

}
