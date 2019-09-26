package io.doolse.simpledba.dynamodb

import java.util.concurrent.CompletableFuture

import io.doolse.simpledba.{JavaEffects, StreamEffects}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

case class DynamoDBEffect[S[-_, _], F[-_, _], R](S: StreamEffects[S, F], asyncClient: F[R, DynamoDbAsyncClient])(implicit JE: JavaEffects[F])
{
  def fromFuture[A](future: => CompletableFuture[A]):F[Any, A] = JE.fromFuture(future)

  def void[R1 <: R](f: F[R1, _]): F[R1, Unit] = S.mapF(f)(_ => ())
}
