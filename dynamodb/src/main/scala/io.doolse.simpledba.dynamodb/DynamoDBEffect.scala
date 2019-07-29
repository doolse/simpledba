package io.doolse.simpledba.dynamodb

import java.util.concurrent.CompletableFuture

import io.doolse.simpledba.{JavaEffects, StreamEffects}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

case class DynamoDBEffect[S[-_, _], F[-_, _], R](asyncClient: F[R, DynamoDbAsyncClient], S: StreamEffects[S, F])(implicit JE: JavaEffects[F])
{
  def fromFuture[A](future: => CompletableFuture[A]):F[Any, A] = JE.fromFuture(() => future)

  def void[R1 <: R](f: F[R1, _]): F[R1, Unit] = S.mapF(f)(_ => ())
}
