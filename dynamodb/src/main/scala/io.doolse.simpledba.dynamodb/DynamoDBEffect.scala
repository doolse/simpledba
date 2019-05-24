package io.doolse.simpledba.dynamodb

import java.util.concurrent.CompletableFuture

import cats.Monad
import io.doolse.simpledba.{JavaEffects, Streamable}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

case class DynamoDBEffect[S[_], F[_]](asyncClient: F[DynamoDbAsyncClient])(
  implicit val S: Streamable[S, F], val M : Monad[F],
  JE: JavaEffects[F])
{
  def fromFuture[A](future: => CompletableFuture[A]):F[A] = JE.fromFuture(() => future)
}
