package io.doolse.simpledba.test.dynamodb

import java.net.URI
import java.util.UUID

import cats.Monad
import cats.syntax.flatMap._
import cats.syntax.functor._
import io.doolse.simpledba.dynamodb.{DynamoDBEffect, DynamoDBMapper, DynamoDBPKColumn, DynamoDBTable, DynamoDBWriteOp}
import io.doolse.simpledba.Iso
import io.doolse.simpledba.test.TestEffects
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, AwsCredentialsProvider, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import zio.ZIO

trait DynamoDBTestHelper[S[_], F[_]] extends TestEffects[S, F] {
  implicit def uuidIso = Iso.uuidString
  implicit def uuidKey = new DynamoDBPKColumn[UUID]

  def localClient = {
    val builder = DynamoDbAsyncClient.builder()
    builder
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
      .region(Region.US_EAST_1)
      .endpointOverride(URI.create("http://localhost:8000"))
      .build()
  }

  def effect: DynamoDBEffect[S, F]

  def attempt[A](f: F[A]): F[Either[Throwable, A]]

  lazy val mapper = new DynamoDBMapper(effect)

  def flush(w: S[DynamoDBWriteOp]): F[Unit] = mapper.flush(w)

  def delAndCreate(table: DynamoDBTable): F[Unit] = {
    implicit val _M = sync
    for {
      client <- effect.asyncClient
      _ <- attempt(effect.fromFuture {
        client.deleteTable(DeleteTableRequest.builder().tableName(table.name).build())
      })
      _ <- effect.fromFuture {
        client.createTable(table.tableDefiniton.build())
      }
    } yield ()
  }

}
