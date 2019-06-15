package io.doolse.simpledba.test.jdbc

import cats.effect.{IO, Sync}
import io.doolse.simpledba.{JavaEffects, Streamable}
import io.doolse.simpledba.fs2._

trait FS2Properties {
  def S : Streamable[fs2.Stream[IO, ?], IO] = implicitly[Streamable[fs2.Stream[IO, ?], IO]]
  def JE : JavaEffects[IO] = implicitly[JavaEffects[IO]]
  def Sync : Sync[IO] = implicitly[Sync[IO]]
  def run[A](prog: IO[A]): A = prog.unsafeRunSync()
}
