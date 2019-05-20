package io.doolse.simpledba.dynamodb.test

import java.net.URI
import java.util.concurrent.CompletableFuture

import cats.Monad
import cats.effect.{ExitCode, IO, IOApp}
import fs2._
import io.doolse.simpledba.dynamodb.{DynamoDBEffect, DynamoDBMapper, DynamoDBTable}
import io.doolse.simpledba.test.Test
import io.doolse.simpledba.test.Test._
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import shapeless.syntax.singleton._

case class MyTest(name: String, frogs: Int)

object DynamoDBTest extends IOApp {
  val builder = DynamoDbAsyncClient.builder()

  val localClient =
    builder.region(Region.US_EAST_1).endpointOverride(URI.create("http://localhost:8000")).build()

  private val effect: DynamoDBEffect[IO] = new DynamoDBEffect[IO] {
    def M                                             = Monad[IO]
    override def asyncClient: IO[DynamoDbAsyncClient] = IO.pure(localClient)

    case object EmptyValue extends Throwable

    override def fromFuture[A](future: => CompletableFuture[A]): IO[A] = IO.delay(future).flatMap {
      cf =>
        IO.async { cb =>
          cf.handle[Unit] { (value: A, t: Throwable) =>
            if (t != null) cb(Left(t))
            else if (value != null) cb(Right(value))
            else cb(Left(EmptyValue))
          }
        }
    }
  }
  val mapper = new DynamoDBMapper[IO](effect)

  implicit val embedded = mapper.mapped[EmbeddedFields].embedded
  val userLNTable       = mapper.mapped[User].table("userLN", 'lastName, 'firstName)
  val userTable =
    mapper.mapped[User].table("user", 'firstName, 'lastName).withLocalIndex('yearIndex, 'year)
  val instTable = mapper.mapped[Inst].table("inst", 'uniqueid)

  def delAndCreate(table: DynamoDBTable): Stream[IO, Unit] = Stream.eval {
    for {
      client <- effect.asyncClient
      _ <- effect.fromFuture {
        client.deleteTable(DeleteTableRequest.builder().tableName(table.name).build())
      }.attempt
      _ <- effect.fromFuture {
        client.createTable(table.tableDefiniton.build())
      }
    } yield ()
  }

  def run(args: List[String]): IO[ExitCode] = {
    val q = mapper.queries
    import q._
    implicit val flusher = mapper.flusher
    val writeInst        = writes(instTable)
    val queries = Queries[IO](
      Stream(userTable, userLNTable, instTable).flatMap(delAndCreate).compile.drain,
      writeInst,
      writes(userTable, userLNTable),
      f => {
        val inst = f(1L)
        flusher.flush(writeInst.insert(inst)).map(_ => inst)
      },
      get(instTable).build,
      queryIndex(userTable, 'yearIndex).build(true),
      query(userLNTable).build(false),
      get(userTable).build,
      o => Stream.empty,
      o => Stream.empty
    )

    val prog = for {
      _   <- Stream.eval(queries.initDB)
      res <- Test.doTest(queries)
    } yield res

    prog.compile.last
      .map(res => { println(res); ExitCode.Success })
  }

}
