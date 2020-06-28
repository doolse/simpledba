package io.doolse.simpledba.test.zio

import cats.{FlatMap, Monad}
import cats.effect.Sync
import io.doolse.simpledba.interop.zio._
import io.doolse.simpledba.test.TestEffects
import io.doolse.simpledba.{JavaEffects, Streamable}
import zio.interop.catz._
import zio.stream._
import zio.{BootstrapRuntime, Task, ZIO}

trait ZIOProperties extends TestEffects[Stream[Throwable, *], Task] {
  def streamable = implicitly[Streamable[Stream[Throwable, *], Task]]
  def sync = implicitly[Sync[Task]]
  def javaEffects = implicitly[JavaEffects[Task]]

  def SM  = new Monad[Stream[Throwable, *]] {
    override def flatMap[A, B](fa: Stream[Throwable, A])(f: A => Stream[Throwable, B]): Stream[Throwable, B] = fa.flatMap(f)

    override def tailRecM[A, B](a: A)(f: A => Stream[Throwable, Either[A, B]]): Stream[Throwable, B] = ???

    override def map[A, B](fa: Stream[Throwable, A])(f: A => B): Stream[Throwable, B] = fa.map(f)

    override def pure[A](x: A): Stream[Throwable, A] = ZStream(x)
  }
  def attempt[A](f: Task[A]) : Task[Either[Throwable, A]] = f.fold(Left.apply, Right.apply)
  lazy val runtime = new BootstrapRuntime {}
  def run[A](prog: Task[A]): A = runtime.unsafeRun(prog)

  def toVector[A](s: ZStream[Any, Throwable, A]): ZIO[Any, Throwable, Vector[A]] =
    s.run(ZSink.collectAll[A]).map(_.toVector)
  def last[A](s: ZStream[Any, Throwable, A]): ZIO[Any, Throwable, Option[A]] =
    s.runLast

}
