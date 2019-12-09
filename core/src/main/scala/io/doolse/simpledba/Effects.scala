package io.doolse.simpledba

import java.util.concurrent.CompletableFuture

import cats.Monad
import cats.effect.IO

trait JavaEffects[F[_]] {
  def blockingIO[A](thunk: => A): F[A]
  def fromFuture[A](future: => CompletableFuture[A]): F[A]
}
trait Streamable[S[_], F[_]] {

  def eval[A](fa: F[A]): S[A]
  def evalMap[A, B](sa: S[A])(f: A => F[B]): S[B]
  def empty[A]: S[A]
  def emit[A](a: A): S[A]
  def emits[A](a: Seq[A]): S[A]
  def foldLeft[O, O2](s: S[O], z: O2)(f: (O2, O) => O2): S[O2]

  def append[A](a: S[A], b: S[A]): S[A]
  def bracket[A](acquire: F[A])(release: A => F[Unit]): S[A]
  def mapS[A, B](sa: S[A])(f: A => B): S[B]
  def flatMapS[A, B](sa: S[A])(f: A => S[B]): S[B]
  def read[A, B](acquire: F[A])(release: A => F[Unit])(
    read: A => F[Option[B]]): S[B] = {
    val s = bracket(acquire)(release)
    def loop(a: A): S[B] = flatMapS(eval(read(a))) {
        case None    => empty
        case Some(b) => append(emit(b), loop(a))
      }
    flatMapS(s)(loop)
  }
  def maxMapped[A, B](n: Int, s: S[A])(f: Seq[A] => B): S[B]

  def read1[A](s: S[A]): F[A]

  def drain(s: S[_]): F[Unit]
}

object JavaEffects {
  implicit val catsIOJavaEffects: JavaEffects[IO] = new JavaEffects[IO] {

    case object EmptyValue extends Throwable

    override def blockingIO[A](thunk: => A): IO[A] = IO.delay(thunk)

    override def fromFuture[A](future: => CompletableFuture[A]): IO[A] = IO.async { cb =>
      future.handle[Unit] { (value: A, t: Throwable) =>
        if (t != null) cb(Left(t))
        else if (value != null) cb(Right(value))
        else cb(Left(EmptyValue))
      }
    }

  }
}
