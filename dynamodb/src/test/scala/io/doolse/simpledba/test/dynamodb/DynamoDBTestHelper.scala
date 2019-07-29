package io.doolse.simpledba.test.dynamodb

import java.net.URI
import java.util.UUID

import cats.Monad
import cats.syntax.flatMap._
import cats.syntax.functor._
import io.doolse.simpledba.dynamodb.{DynamoDBEffect, DynamoDBMapper, DynamoDBPKColumn, DynamoDBTable, DynamoDBWriteOp}
import io.doolse.simpledba.Iso
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, AwsCredentialsProvider, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest

trait DynamoDBTestHelper[SR[-_, _], FR[-_, _]] {
  def SM : Monad[SR[Any, ?]]
  def M : Monad[FR[Any, ?]]
  implicit def uuidIso = Iso.uuidString
  implicit def uuidKey = new DynamoDBPKColumn[UUID]

  lazy val localClient = {
    val builder = DynamoDbAsyncClient.builder()
    builder
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
      .region(Region.US_EAST_1)
      .endpointOverride(URI.create("http://localhost:8000"))
      .build()
  }

  def effect: DynamoDBEffect[SR, FR, Any]

  def attempt[A](f: FR[Any, A]): FR[Any, Either[Throwable, A]]

  lazy val mapper = new DynamoDBMapper(effect)

  def flush(w: SR[Any, DynamoDBWriteOp]): FR[Any, Unit] = mapper.flush(w)

  def delAndCreate(table: DynamoDBTable): FR[Any, Unit] = {
    implicit val _M = M
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
