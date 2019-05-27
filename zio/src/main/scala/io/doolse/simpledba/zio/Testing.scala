package io.doolse.simpledba.zio

import scalaz.zio._
import scalaz.zio.console._
import scalaz.zio.stream.{ZSink, ZStream}

object Testing extends App {

  def onceOnly[A, R, E] : A => ZIO[R, E, Option[A]] = {
    var first = true
    a => if (first) {
      first = false
      ZIO.succeed(Some(a))
    } else ZIO.succeed(None)
  }

  override def run(args: List[String]): ZIO[Testing.Environment, Nothing, Int] = {
    val connection = ZStream.bracket(putStrLn("ACQUIRED"))(_ => ZIO.succeedLazy(println("RELEASED")))(onceOnly)

    val prog = for {
      con <- connection
      _ <- ZStream.fromEffect(putStrLn(s"DOING ${con}"))
    } yield ()
    prog.run(ZSink.drain).fold(_ => 0, _ => 1)

  }

}
