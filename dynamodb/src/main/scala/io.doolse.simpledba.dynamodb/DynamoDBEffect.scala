package io.doolse.simpledba.dynamodb

import java.util.concurrent.CompletableFuture

import cats.Monad
import io.doolse.simpledba.Streamable
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

trait DynamoDBEffect[S[_[_], _], F[_]] {

  def S: Streamable[S, F]

  def M: Monad[F]

  def asyncClient: F[DynamoDbAsyncClient]

  def fromFuture[A](future: => CompletableFuture[A]): F[A]

}

