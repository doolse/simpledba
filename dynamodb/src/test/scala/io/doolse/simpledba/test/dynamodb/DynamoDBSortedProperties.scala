package io.doolse.simpledba.test.dynamodb

import java.util.UUID

import cats.instances.vector._
import cats.syntax.foldable._
import io.doolse.simpledba.dynamodb._
import io.doolse.simpledba.interop.zio._
import io.doolse.simpledba.test.zio.ZIOProperties
import io.doolse.simpledba.test.{SafeString, SimpleDBAProperties, Sortable, SortedQueryProperties}
import software.amazon.awssdk.core.SdkBytes
import zio.interop.catz._
import zio.stream._
import zio.{Task, ZIO}

import scala.collection.mutable

object DynamoDBSortedProperties extends SimpleDBAProperties("DynamoDB") {

  include(
    new SortedQueryProperties[StreamTask, Task, DynamoDBWriteOp]() with ZIOProperties
    with DynamoDBTestHelper[StreamTask, Task] {

      override def effect = DynamoDBEffect[StreamTask, Task](ZIO.succeed(localClient))

      lazy val baseTable = mapper.mapped[Sortable].table("sort").partKey('same)

      val tables = mutable.Buffer[DynamoDBTable.SameT[Sortable]]()

      def table(f: Sortable => SdkBytes) = {
        val newt = baseTable.derivedSortKey(f).copy(name = "sort_" + tables.size)
        tables += newt
        newt
      }

      implicit val safeStringKey = BinaryKey.stringBinaryKey.cmap[SafeString](_.s)

      val int1 = table(s => BinaryKey(s.intField -> s.pk1))
      val int2 = table(s => BinaryKey((s.intField, s.stringField, s.pk1)))
      val string1 = table(s => BinaryKey(s.stringField -> s.pk1))
      val string2 = table(s => BinaryKey((s.stringField, s.shortField, s.pk1)))
      val short1 = table(s => BinaryKey(s.shortField -> s.pk1))
      val short2 = table(s => BinaryKey((s.shortField, s.uuidField, s.pk1)))
      val long1 = table(s => BinaryKey(s.longField -> s.pk1))
      val long2 = table(s => BinaryKey((s.longField, s.floatField, s.pk1)))
      val float1 = table(s => BinaryKey(s.floatField -> s.pk1))
      val float2 = table(s => BinaryKey((s.floatField, s.uuidField, s.pk1)))
      val uuid1 = table(s => BinaryKey(s.uuidField -> s.pk1))
      val uuid2 = table(s => BinaryKey((s.uuidField, s.uuidField, s.pk1)))

      def mkQueries(asc: Boolean): Queries = {
        val q = mapper.queries

        def query(t: DynamoDBSortTable.Aux[Sortable, UUID, _]) =
          q.query(t).build[UUID](asc)

        Queries(
          query(int1),
          query(int2),
          query(string1),
          query(string2),
          query(short1),
          query(short2),
          query(long1),
          query(long2),
          query(float1),
          query(float2),
          query(uuid1),
          query(uuid2),
          q.writes(tables.toSeq: _*),
          Stream.empty
        )
      }

      override val queries: (Queries, Queries) = {
        run(tables.toVector.traverse_(delAndCreate))
        (mkQueries(true), mkQueries(false))
      }

    })
}
