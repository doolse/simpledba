package io.doolse.simpledba.test

import cats.effect.Sync
import io.doolse.simpledba.{JavaEffects, Streamable}
import io.doolse.simpledba.zio._
import scalaz.zio.{DefaultRuntime, Task}
import scalaz.zio.stream.ZStream
import scalaz.zio.interop.catz._

trait ZIOProperties {
  def S : Streamable[ZStream[Any, Throwable, ?], Task] = implicitly[Streamable[ZStream[Any, Throwable, ?], Task]]
  def JE : JavaEffects[Task] = implicitly[JavaEffects[Task]]
  def Sync : Sync[Task] = implicitly[Sync[Task]]
  lazy val runtime = new DefaultRuntime {}
  def run[A](prog: Task[A]): A = runtime.unsafeRun(prog)
}
