package io.doolse.simpledba

import cats.Monad
import cats.data._
import fs2.util.{Catchable, Task}
import io.doolse.simpledba.dynamodb.DynamoDBMapper.Effect

/**
  * Created by jolz on 14/06/16.
  */
package object dynamodb {

  implicit def dynamoDBCatchable(implicit M: Monad[Effect], CT: Catchable[Task]): Catchable[Effect] = new Catchable[Effect] {

    def fail[A](err: Throwable): Effect[A] = ReaderT (_ => CT.fail(err))

    def attempt[A](fa: Effect[A]): Effect[Either[Throwable, A]] = ReaderT(s => CT.attempt(fa.run(s)))

    def bind[A, B](a: Effect[A])(f: (A) => Effect[B]): Effect[B] = M.flatMap(a)(f)

    def pure[A](a: A): Effect[A] = M.pure(a)
  }

}
