package io.doolse.simpledba

import cats.Monad
import cats.data._
import fs2.util.{Catchable, Task}
import io.doolse.simpledba.cassandra.CassandraMapper.Effect

/**
  * Created by jolz on 10/06/16.
  */
package object cassandra {

  implicit def cassandraCatchable(implicit M: Monad[Effect], CT: Catchable[Task]) : Catchable[Effect] = new Catchable[Effect] {
    def fail[A](err: Throwable): Effect[A] = ReaderT { s => CT.fail[A](err) }

    def attempt[A](fa: Effect[A]): Effect[Either[Throwable, A]] = ReaderT { s => CT.attempt(fa.run(s)) }

    def bind[A, B](a: Effect[A])(f: (A) => Effect[B]): Effect[B] = M.flatMap(a)(f)

    def pure[A](a: A): Effect[A] = M.pure(a)
  }

}
