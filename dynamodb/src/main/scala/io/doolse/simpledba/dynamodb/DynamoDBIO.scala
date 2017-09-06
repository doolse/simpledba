package io.doolse.simpledba.dynamodb

import java.util

import cats.data.{Kleisli, StateT, WriterT}
import cats.effect.IO
import fs2.{Pipe, Pure, Scheduler, Stream}
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.amazonaws.services.dynamodbv2.model._
import io.doolse.simpledba.dynamodb.DynamoDBIO._
import io.doolse.simpledba.CatsUtils._
import io.doolse.simpledba.WriteOp

import scala.concurrent.ExecutionContext
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by jolz on 29/06/16.
  */
object DynamoDBIO {
  // implicit val scheduler = Scheduler.fromFixedDaemonPool(4)

  type AsyncCall[R <: AmazonWebServiceRequest, A] = AmazonDynamoDBAsync => (R, AsyncHandler[R, A]) => java.util.concurrent.Future[A]

  def asyncReq[R <: AmazonWebServiceRequest, A](f: AsyncCall[R, A])(client: AmazonDynamoDBAsync, r: R): IO[A] = {
    IO.async[A] {
      cb => f(client)(r, new AsyncHandler[R, A] {
        def onError(exception: Exception): Unit = cb(Left(exception))

        def onSuccess(request: R, result: A): Unit = cb(Right(result))
      })
    }
  }

  val queryAsync: AsyncCall[QueryRequest, QueryResult] = _.queryAsync
  val scanAsync: AsyncCall[ScanRequest, ScanResult] = _.scanAsync
  val getItemAsync: AsyncCall[GetItemRequest, GetItemResult] = _.getItemAsync
  val batchGetItemAsync: AsyncCall[BatchGetItemRequest, BatchGetItemResult] = _.batchGetItemAsync
  val deleteItemAsync: AsyncCall[DeleteItemRequest, DeleteItemResult] = _.deleteItemAsync
  val batchWriteItemAsync: AsyncCall[BatchWriteItemRequest, BatchWriteItemResult] = _.batchWriteItemAsync
  val putItemAsync: AsyncCall[PutItemRequest, PutItemResult] = _.putItemAsync
  val updateItemAsync: AsyncCall[UpdateItemRequest, UpdateItemResult] = _.updateItemAsync
  val describeTableAsync: AsyncCall[DescribeTableRequest, DescribeTableResult] = _.describeTableAsync
  val listTablesAsync: AsyncCall[ListTablesRequest, ListTablesResult] = _.listTablesAsync
  val deleteTableAsync: AsyncCall[DeleteTableRequest, DeleteTableResult] = _.deleteTableAsync
  val createTableAsync: AsyncCall[CreateTableRequest, CreateTableResult] = _.createTableAsync

  def queryDynamo[A](f: DynamoDBSession => IO[A]): Effect[A] = StateT.inspectF(f)
  def writeDynamo(w: DynamoDBWrite): Stream[Effect, WriteOp] = Stream.emit(w)

  def batchWriteResultStream(qr: BatchWriteItemRequest): Stream[Effect, Unit] = {
    Stream.eval[Effect, BatchWriteItemResult](queryDynamo {
      _.request(batchWriteItemAsync, qr)
    }).flatMap { result =>
      if (result.getUnprocessedItems.isEmpty) Stream.emit()
      else batchWriteResultStream(qr.withRequestItems(result.getUnprocessedItems))
    }
  }

  val writePipe : Pipe[Effect, WriteOp, Int] = { w =>
    w.buffer(25).chunks.flatMap { c =>
      val batchWriteMapJava = new java.util.HashMap[String, java.util.List[WriteRequest]]
      val m = batchWriteMapJava.asScala
      val b = mutable.Buffer[UpdateItemRequest]()
      c.foreach {
        case DynamoDBBatchable(n, wr) => m.getOrElseUpdate(n, new util.ArrayList[WriteRequest]).add(wr)
        case DynamoDBUpdate(uir) => b += uir
      }
      val updates = Stream.emits(b).evalMap {
        uir => queryDynamo(_.request(updateItemAsync, uir).map(_ => 1))
      }
      val batch = if (m.isEmpty) Stream.empty.covary[Effect] else batchWriteResultStream(new BatchWriteItemRequest(batchWriteMapJava)).map(_ => 1)
      batch ++ updates
    }
  }
}

sealed trait DynamoDBWrite extends WriteOp
case class DynamoDBBatchable(table: String, dr: WriteRequest) extends DynamoDBWrite
case class DynamoDBUpdate(uir: UpdateItemRequest) extends DynamoDBWrite

case class DynamoDBSession(client: AmazonDynamoDBAsync, logger: (() ⇒ String) ⇒ Unit = _ => ()) {
  def request[R <: AmazonWebServiceRequest, A](f: AsyncCall[R, A], req: R): IO[A] = {
    logger(() => req.toString)
    asyncReq(f)(client, req)
  }

}
