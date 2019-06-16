package io.doolse.simpledba.test.dynamodb

import java.net.URI
import java.util.UUID

import cats.syntax.flatMap._
import cats.syntax.functor._
import io.doolse.simpledba.dynamodb.{DynamoDBEffect, DynamoDBMapper, DynamoDBPKColumn, DynamoDBTable}
import io.doolse.simpledba.{Flushable, Iso}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest

trait DynamoDBTestHelper[S[_], F[_]] {

  implicit def uuidIso = Iso.uuidString
  implicit def uuidKey = new DynamoDBPKColumn[UUID]

  lazy val localClient = {
    val builder = DynamoDbAsyncClient.builder()
    builder.region(Region.US_EAST_1).endpointOverride(URI.create("http://localhost:8000")).build()
  }

  def effect: DynamoDBEffect[S, F]

  def attempt[A](f: F[A]): F[Either[Throwable, A]]

  lazy val mapper = new DynamoDBMapper(effect)

  def flushable: Flushable[S] = mapper.flusher

  def delAndCreate(table: DynamoDBTable): F[Unit] = {
    implicit val M = effect.S.M
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
