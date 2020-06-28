package io.doolse.simpledba.test.dynamodb
import java.net.URI

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import zio.{BootstrapRuntime, ExitCode, Task, URIO, ZIO}
import zio.console._

object TestError extends App {

  lazy val runtime = new BootstrapRuntime { }

  def run[A](prog: URIO[zio.ZEnv, A]): A = runtime.unsafeRun(prog)
  def attempt[A](f: Task[A]) = f.fold(Left.apply, Right.apply)


  val thing: URIO[zio.ZEnv, ExitCode] = {
    val builder = DynamoDbAsyncClient.builder()
    (for {
      client <- ZIO(builder
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
        .region(Region.US_EAST_1)
        .endpointOverride(URI.create("http://localhost:8000"))
        .build())
      a <- attempt(Task.fromCompletionStage(client.deleteTable(DeleteTableRequest.builder().tableName("another_table").build())))
      _ <- putStrLn(a.toString)
    } yield ExitCode.success).orDie
  }

  run(thing)
}
