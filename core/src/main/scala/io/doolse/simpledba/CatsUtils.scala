package io.doolse.simpledba

import cats.data._
import cats.{Applicative, Monad}
import fs2.util.Async.Ref
import fs2.util._
import fs2.{Strategy, Task}

import scala.concurrent.ExecutionContext

/**
  * Created by jolz on 29/06/16.
  */
object CatsUtils {

  def whenM[F[_], A](b: Boolean, fa: => F[A])(implicit M: Applicative[F]): F[Unit] = if (b) M.map(fa)(_ => ()) else M.pure()

//  implicit def readerCatchable[S](implicit M: Monad[ReaderT[Task, S, ?]], S: Strategy, A: Async[Task]) = new Async[ReaderT[Task, S, ?]] {
//    def ref[A]: ReaderT[Task, S, Ref[({type Λ$[γ] = ReaderT[Task, S, γ]})#Λ$, A]] = ???
//
//    def unsafeRunAsync[A](fa: ReaderT[Task, S, A])(cb: (Attempt[A]) => Unit): Unit = ???
//
//    def suspend[A](fa: => ReaderT[Task, S, A]): ReaderT[Task, S, A] = ???
//
//    def fail[A](err: Throwable): ReaderT[Task, S, A] = ???
//
//    def attempt[A](fa: ReaderT[Task, S, A]): ReaderT[Task, S, Attempt[A]] = ???
//
//    def flatMap[A, B](a: ReaderT[Task, S, A])(f: (A) => ReaderT[Task, S, B]): ReaderT[Task, S, B] = ???
//
//    def pure[A](a: A): ReaderT[Task, S, A] = ???
//  }

}
