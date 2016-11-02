package io.doolse.simpledba.dynamodb

import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.amazonaws.services.dynamodbv2.model._
import fs2.{Scheduler, Strategy, Task}
import io.doolse.simpledba.dynamodb.DynamoDBIO._

import scala.concurrent.ExecutionContext

/**
  * Created by jolz on 29/06/16.
  */
object DynamoDBIO {
  implicit val strat = Strategy.fromExecutionContext(ExecutionContext.global)
  implicit val scheduler = Scheduler.fromFixedDaemonPool(4)

  type AsyncCall[R <: AmazonWebServiceRequest, A] = AmazonDynamoDBAsync => (R, AsyncHandler[R, A]) => java.util.concurrent.Future[A]

  def asyncReq[R <: AmazonWebServiceRequest, A](f: AsyncCall[R, A])(client: AmazonDynamoDBAsync, r: R): Task[A] = {
    Task.async[A] {
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
  val putItemAsync: AsyncCall[PutItemRequest, PutItemResult] = _.putItemAsync
  val updateItemAsync: AsyncCall[UpdateItemRequest, UpdateItemResult] = _.updateItemAsync
  val describeTableAsync: AsyncCall[DescribeTableRequest, DescribeTableResult] = _.describeTableAsync
  val listTablesAsync: AsyncCall[ListTablesRequest, ListTablesResult] = _.listTablesAsync
  val deleteTableAsync: AsyncCall[DeleteTableRequest, DeleteTableResult] = _.deleteTableAsync
  val createTableAsync: AsyncCall[CreateTableRequest, CreateTableResult] = _.createTableAsync
}

case class DynamoDBSession(client: AmazonDynamoDBAsync, logger: (() ⇒ String) ⇒ Unit = _ => ()) {
  def request[R <: AmazonWebServiceRequest, A](f: AsyncCall[R, A], req: R): Task[A] = {
    logger(() => req.toString)
    asyncReq(f)(client, req)
  }

}
