package io.doolse.simpledba.dynamodb

import java.util.concurrent.CompletableFuture

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

trait DynamoDBEffect[F[_]] {

  def asyncClient: F[DynamoDbAsyncClient]

  def fromFuture[A](future: => CompletableFuture[A]): F[A]
}
