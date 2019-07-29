package io.doolse.simpledba

import java.util.concurrent.CompletableFuture

import cats.Monad
import cats.effect.IO

trait JavaEffects[F[-_, _]] {
  def blockingIO[A](thunk: => A): F[Any, A]
  def fromFuture[A](future: () => CompletableFuture[A]): F[Any, A]
}

trait IOEffects[F[-_, _]]
{
  val unit : F[Any, Unit]
  def delay[A](a: => A): F[Any, A]
  def flatMapF[R, R1 <: R, A, B](fa: F[R, A])(fb: A => F[R1, B]): F[R1, B]
  def mapF[R, A, B](fa: F[R, A])(f: A => B): F[R, B]
  def productR[R, R1 <: R, A, B](l: F[R, A])(r: F[R1, B]): F[R1, B]
}

trait StreamEffects[S[-_, _], F[-_, _]] extends IOEffects[F] {
  def mapS[R, A, B](fa: S[R, A])(f: A => B): S[R, B]
  def flatMapS[R, R1 <: R, A, B](fa: S[R, A])(fb: A => S[R1, B]): S[R1, B]
  def eval[R, A](fa: F[R, A]): S[R, A]
  def evalMap[R, R1 <: R, A, B](sa: S[R, A])(f: A => F[R1, B]): S[R1, B]
  def empty[R, A]: S[R, A]
  def emit[A](a: A): S[Any, A]
  def emits[A](a: Seq[A]): S[Any, A]
  def foldLeft[R, O, O2](s: S[R, O], z: O2)(f: (O2, O) => O2): S[R, O2]
  def append[R, R1 <: R, A](a: S[R, A], b: S[R1, A]): S[R1, A]
  def bracket[R, A](acquire: F[R, A])(release: A => F[R, Unit]): S[R, A]
  def read[R, A, B](acquire: F[R, A])(release: A => F[R, Unit])(
    read: A => F[R, Option[B]]): S[R, B] = {
    val s = bracket(acquire)(release)
    def loop(a: A): S[R, B] = flatMapS(eval(read(a))) {
        case None    => empty
        case Some(b) => append(emit(b), loop(a))
      }
    flatMapS(s)(loop)
  }
  def maxMapped[R, A, B](n: Int, s: S[R, A])(f: Seq[A] => B): S[R, B]

  def read1[R, A](s: S[R, A]): F[R, A]

  def drain[R](s: S[R, _]): F[R, Unit]
}

object JavaEffects {
  type IOR[-R, A] = IO[A]
  implicit val catsIOJavaEffects: JavaEffects[IOR] = new JavaEffects[IOR] {

    case object EmptyValue extends Throwable

    override def blockingIO[A](thunk: => A): IO[A] = IO.delay(thunk)

    override def fromFuture[A](future: () => CompletableFuture[A]): IO[A] = IO.async { cb =>
      future().handle[Unit] { (value: A, t: Throwable) =>
        if (t != null) cb(Left(t))
        else if (value != null) cb(Right(value))
        else cb(Left(EmptyValue))
      }
    }

  }
}
