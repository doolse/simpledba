package io.doolse.simpledba.test.zio

import cats.effect.Sync
import io.doolse.simpledba.{JavaEffects, Streamable}
import zio.stream._
import zio.{DefaultRuntime, Task, ZIO}
import zio.interop.catz._
import io.doolse.simpledba.interop.zio._

trait ZIOProperties {
  type S[A] = Stream[Throwable, A]
  def streamable : Streamable[S, Task] = implicitly[Streamable[S, Task]]
  def JE : JavaEffects[Task] = implicitly[JavaEffects[Task]]
  def Sync : Sync[Task] = implicitly[Sync[Task]]
  def attempt[A](f: Task[A]) : Task[Either[Throwable, A]] = f.fold(Left.apply, Right.apply)
  lazy val runtime = new DefaultRuntime {}
  def run[A](prog: Task[A]): A = runtime.unsafeRun(prog)

  def toVector[A](s: ZStream[Any, Throwable, A]): ZIO[Any, Throwable, Vector[A]] =
    s.run(ZSink.collectAll[A]).map(_.toVector)
  def last[A](s: ZStream[Any, Throwable, A]): ZIO[Any, Throwable, Option[A]] =
    s.run(ZSink.identity[A].?)

}
