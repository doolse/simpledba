package io.doolse.simpledba.test.jdbc

import cats.Monad
import cats.effect.{IO, Sync}
import io.doolse.simpledba.{JavaEffects, StreamEffects}
import io.doolse.simpledba.fs2._

trait FS2Properties {
  type StreamR[-R, A] = fs2.Stream[IO, A]
  type IOR[-R, A] = IO[A]
  def streamable : StreamEffects[StreamR, IOR] = fs2Stream[IO]
  def javaEffects : JavaEffects[IOR] = implicitly[JavaEffects[IOR]]
  def M = implicitly[Monad[IO]]
  def SM = implicitly[Monad[fs2.Stream[IO, ?]]]
  def Sync : Sync[IO] = implicitly[Sync[IO]]
  def run[A](prog: IO[A]): A = prog.unsafeRunSync()

  def toVector[A](s: fs2.Stream[IO, A]): IO[Vector[A]] = s.compile.toVector

  def last[A](s: fs2.Stream[IO, A]): IO[Option[A]] = s.compile.last

}
