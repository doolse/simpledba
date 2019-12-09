package io.doolse.simpledba.test.jdbc

import cats.Monad
import cats.effect.{IO, Sync}
import io.doolse.simpledba.{JavaEffects, Streamable}
import io.doolse.simpledba.fs2._
import io.doolse.simpledba.test.TestEffects

trait FS2Properties extends TestEffects[fs2.Stream[IO, *], IO] {
  def streamable = implicitly[Streamable[fs2.Stream[IO, *], IO]]
  def javaEffects : JavaEffects[IO] = implicitly[JavaEffects[IO]]
  def SM = implicitly[Monad[fs2.Stream[IO, *]]]
  def sync : Sync[IO] = implicitly[Sync[IO]]
  def run[A](prog: IO[A]): A = prog.unsafeRunSync()

  def toVector[A](s: fs2.Stream[IO, A]): IO[Vector[A]] = s.compile.toVector

  def last[A](s: fs2.Stream[IO, A]): IO[Option[A]] = s.compile.last

}
