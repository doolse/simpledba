package io.doolse.simpledba.dynamodb

import java.util.concurrent.CompletableFuture

import cats.Monad
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

trait DynamoDBEffect[F[_]] {

  def M: Monad[F]

  def asyncClient: F[DynamoDbAsyncClient]

  def fromFuture[A](future: => CompletableFuture[A]): F[A]

}
