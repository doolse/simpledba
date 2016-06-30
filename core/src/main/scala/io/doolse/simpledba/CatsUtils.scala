package io.doolse.simpledba

import cats.data._
import cats.{Applicative, Monad}
import fs2.util._
import fs2.{Async, Strategy, Task}

import scala.concurrent.ExecutionContext

/**
  * Created by jolz on 29/06/16.
  */
object CatsUtils {

  def whenM[F[_], A](b: Boolean, fa: => F[A])(implicit M: Applicative[F]): F[Unit] = if (b) M.map(fa)(_ => ()) else M.pure()

  implicit def readerCatchable[S](implicit M: Monad[ReaderT[Task, S, ?]], S: Strategy, A: Async[Task]) = new Async[ReaderT[Task, S, ?]] {

    type Effect[A] = ReaderT[Task, S, A]

    def fail[A](err: Throwable): Effect[A] = ReaderT(_ => A.fail(err))

    def attempt[A](fa: Effect[A]): Effect[Either[Throwable, A]] = ReaderT(s => A.attempt(fa.run(s)))

    def bind[A, B](a: Effect[A])(f: (A) => Effect[B]): Effect[B] = M.flatMap(a)(f)

    def pure[A](a: A): Effect[A] = M.pure(a)

    type Ref[A] = Task.Ref[A]

    def ref[A] = ReaderT(_ => Task.ref[A])

    def access[A](r: Ref[A]): Effect[(A, (Either[Throwable, A]) => Effect[Boolean])] = ReaderT {
      _ => r.access.map {
        case (a, f) =>
          val newf = (e: Either[Throwable, A]) => ReaderT { (_: S) => f(e) }
          (a, newf)
      }
    }

    def set[A](r: Ref[A])(a: Effect[A]): Effect[Unit] = ReaderT { s => r.set(a(s)) }

    def setFree[A](r: Ref[A])(a: Free[Effect, A]): Effect[Unit] = ReaderT { s => r.setFree(a.translate(new (Effect ~> Task) {
      def apply[A](f: Effect[A]): Task[A] = f(s)
    }))
    }

    def cancellableGet[A](r: Ref[A]): Effect[(Effect[A], Effect[Unit])] = ReaderT { s => r.cancellableGet.map {
      case (a, b) => (ReaderT((_: S) => a), ReaderT((_: S) => b))
    }
    }

    def suspend[A](fa: => Effect[A]): Effect[A] = ReaderT { s => Task.suspend(fa(s)) }

    def runSet[A](q: Ref[A])(a: Either[Throwable,A]): Unit = q.runSet(a)
  }

}
