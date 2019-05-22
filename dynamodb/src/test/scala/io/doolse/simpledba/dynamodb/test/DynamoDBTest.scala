package io.doolse.simpledba.dynamodb.test

import java.net.URI
import java.util.concurrent.CompletableFuture

import cats.Monad
import cats.effect.{ExitCode, IO, IOApp}
import io.doolse.simpledba.{Cols, Streamable}
import io.doolse.simpledba.dynamodb.{DynamoDBEffect, DynamoDBMapper, DynamoDBTable}
import io.doolse.simpledba.test.Test
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import shapeless.syntax.singleton._
import io.doolse.simpledba.fs2._
import fs2._

case class MyTest(name: String, frogs: Int)

object DynamoDBTest extends IOApp with Test[fs2.Stream, IO] {
  val builder = DynamoDbAsyncClient.builder()
  def last[A](s: fs2.Stream[IO, A]) = s.last

  val localClient =
    builder.region(Region.US_EAST_1).endpointOverride(URI.create("http://localhost:8000")).build()

  def S = implicitly[Streamable[fs2.Stream, IO]]

  private val effect: DynamoDBEffect[fs2.Stream, IO] = new DynamoDBEffect[fs2.Stream, IO] {
    def S = implicitly[Streamable[fs2.Stream, IO]]
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

  val mapper = new DynamoDBMapper[fs2.Stream, IO](effect)
  def flusher = mapper.flusher

  implicit val embedded = mapper.mapped[EmbeddedFields].embedded
  val userLNTable       = mapper.mapped[User].table("userLN", 'lastName, 'firstName)
  val userTable =
    mapper
      .mapped[User]
      .table("user", 'firstName, 'lastName)
      .withLocalIndex('yearIndex, 'year)
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
    val queries = Queries(
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
      getAttr(userTable, Cols('year)).buildAs[Username, Int],
      o => Stream.empty
    )

    val prog = for {
      _   <- Stream.eval(queries.initDB)
      res <- doTest(queries)
    } yield res

    prog.compile.last
      .map(res => { println(res); ExitCode.Success })
  }

}
