package io.doolse.simpledba.dynamodb.test

import java.net.URI
import java.util.concurrent.CompletableFuture

import cats.effect.{ExitCode, IO, IOApp}
import io.doolse.simpledba.dynamodb.{DynamoDBEffect, DynamoDBMapper}
import shapeless.syntax.singleton._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, DynamoDbClient}
import software.amazon.awssdk.services.dynamodb.model.{CreateTableRequest, ProvisionedThroughput}

case class MyTest(name: String, frogs: Int)

object DynamoDBTest extends IOApp {
  val builder = DynamoDbAsyncClient.builder()

  val localClient =
    builder.region(Region.US_EAST_1).endpointOverride(URI.create("http://localhost:8000")).build()

  val mapper = new DynamoDBMapper[IO](new DynamoDBEffect[IO] {
    override def asyncClient: IO[DynamoDbAsyncClient] = IO.pure(localClient)

    case object EmptyValue extends Throwable

    override def fromFuture[A](future: => CompletableFuture[A]): IO[A] = {
      IO.delay(future).flatMap { f =>
        IO.async { cb =>
          f.handle[Unit] { (value: A, t: Throwable) =>
            if (t != null) cb(Left(t))
            else if (value != null) cb(Right(value))
            else cb(Left(EmptyValue))
          }
        }
      }
    }
  })

  val simpleTable = mapper.mapped[MyTest].table("hello", 'name)

//  private val definiton: CreateTableRequest = simpleTable.tableDefiniton
//    .build()
//  println(definiton)
//
//  localClient.createTable(definiton)

  def run(args: List[String]): IO[ExitCode] = {
    val q       = mapper.queries
    val flusher = mapper.flusher
    flusher
      .flush(q.writes(simpleTable).insert(MyTest("Something", 1)))
      .compile
      .drain
      .map(_ => ExitCode.Success)
  }

}
