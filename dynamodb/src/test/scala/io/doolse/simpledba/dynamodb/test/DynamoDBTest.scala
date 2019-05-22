package io.doolse.simpledba.dynamodb.test

import java.net.URI

import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import io.doolse.simpledba.{Cols, Flushable}
import io.doolse.simpledba.dynamodb.{DynamoDBEffect, DynamoDBMapper, DynamoDBTable}
import io.doolse.simpledba.test.Test
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest

case class MyTest(name: String, frogs: Int)

trait DynamoDBTest[S[_[_], _], F[_]] extends Test[S, F] {
//  def last[A](s: fs2.Stream[IO, A]) = s.last

  lazy val localClient = {
    val builder = DynamoDbAsyncClient.builder()
    builder.region(Region.US_EAST_1).endpointOverride(URI.create("http://localhost:8000")).build()
  }

//  def S = implicitly[Streamable[fs2.Stream, IO]]

  def effect: DynamoDBEffect[S, F]
  implicit def AE: MonadError[F, Throwable]

//  private val effect: DynamoDBEffect[fs2.Stream, IO] = new DynamoDBEffect[fs2.Stream, IO] {
//    def S = implicitly[Streamable[fs2.Stream, IO]]
//    def M                                             = Monad[IO]
//    override def asyncClient: IO[DynamoDbAsyncClient] = IO.pure(localClient)
//
//    case object EmptyValue extends Throwable
//
//    override def fromFuture[A](future: => CompletableFuture[A]): IO[A] = IO.delay(future).flatMap {
//      cf =>
//        IO.async { cb =>
//          cf.handle[Unit] { (value: A, t: Throwable) =>
//            if (t != null) cb(Left(t))
//            else if (value != null) cb(Right(value))
//            else cb(Left(EmptyValue))
//          }
//        }
//    }
//  }

  val mapper  = new DynamoDBMapper(effect)

  implicit val embedded = mapper.mapped[EmbeddedFields].embedded
  val userLNTable       = mapper.mapped[User].table("userLN", 'lastName, 'firstName)
  val userTable =
    mapper
      .mapped[User]
      .table("user", 'firstName, 'lastName)
      .withLocalIndex('yearIndex, 'year)
  val instTable = mapper.mapped[Inst].table("inst", 'uniqueid)

  def delAndCreate(table: DynamoDBTable): F[Unit] = {
    for {
      client <- effect.asyncClient
      _ <- AE.attempt(effect.fromFuture {
        client.deleteTable(DeleteTableRequest.builder().tableName(table.name).build())
      })
      _ <- effect.fromFuture {
        client.createTable(table.tableDefiniton.build())
      }
    } yield ()
  }

  override def flusher: Flushable[S, F] = mapper.flusher

  val q = mapper.queries
  import q._

  val tables = S.emits(Seq(userTable, userLNTable, instTable))
  val writeInst = writes(instTable)
  val queries = {
    implicit def M = effect.M
    Queries(
      S.drain(S.evalMap(tables)(delAndCreate)),
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
      o => S.empty
    )
  }

  val prog = for {
    _   <- S.eval(queries.initDB)
    res <- doTest(queries)
  } yield res

}
