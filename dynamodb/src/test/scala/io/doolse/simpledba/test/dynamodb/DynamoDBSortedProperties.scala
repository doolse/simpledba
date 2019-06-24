package io.doolse.simpledba.test.dynamodb

import java.util.UUID

import io.doolse.simpledba.Cols
import io.doolse.simpledba.dynamodb.{BEGINS_WITH, CompositeKeyBuilder, DynamoDBEffect, DynamoDBSortTable, DynamoDBTable, FullKeyTable}
import io.doolse.simpledba.test.zio.ZIOProperties
import io.doolse.simpledba.test.{SafeString, SimpleDBAProperties, Sortable, SortedQueryProperties}
import zio.{Task, ZIO}
import zio.stream._
import org.scalacheck.Shapeless._
import io.doolse.simpledba.zio._
import shapeless._
import cats.syntax.foldable._
import cats.instances.vector._

import scala.collection.mutable
import zio.interop.catz._

//case class Sortable(pk1: UUID,
//                    same: UUID,
//                    intField: Int,
//                    stringField: SafeString,
//                    shortField: Short,
//                    longField: Long,
//                    floatField: Float,
//                    doubleField: Double,
//                    uuidField: UUID)
object DynamoDBSortedProperties extends SimpleDBAProperties("DynamoDB") {

  type S[A] = Stream[Throwable, A]
  type F[A] = Task[A]

  include(
    new SortedQueryProperties[S, F]() with ZIOProperties
    with DynamoDBTestHelper[S, F] {

      override def effect = DynamoDBEffect[S, Task](ZIO.succeed(localClient))

      lazy val baseTable = mapper.mapped[Sortable].table("sort").partKey('same)

      val tables = mutable.Buffer[DynamoDBTable.SameT[Sortable]]()

      def table[A](t: A)(implicit ev: A <:< FullKeyTable[Sortable, _ <: HList, _, _, _]): A = {
        val newt = t.copy(name = "sort_" + tables.size)
        tables += newt
        newt.asInstanceOf[A]
      }

      implicit val safeStringComposite = CompositeKeyBuilder.stringComposite.cmap[SafeString](_.s)

      val int1Table = table(baseTable.derivedSortKey(s => CompositeKeyBuilder.toSdkBytes(s.intField -> s.pk1)))
      val int2Table = table(baseTable.derivedSortKey(s => CompositeKeyBuilder.toSdkBytes((s.intField, s.stringField, s.pk1))))


      def mkQueries(asc: Boolean): Queries = {
        val q = mapper.queries
        import q._
//        def prefixQuery(table: DynamoDBSortTable.Aux[Sortable, UUID, String]) : UUID => S[Sortable] = {
//          val q = queryOp(table, BEGINS_WITH).build(asc)
//          sameUuid =>
//        }
        Queries(
          writes(tables: _*),
          Stream.empty,
          query(int1Table).build[UUID](asc),
          query(int2Table).build[UUID](asc),
          _ => ???,
          _ => ???,
          _ => ???,
          _ => ???,
          _ => ???,
          _ => ???,
          _ => ???,
          _ => ???,
          _ => ???,
          _ => ???
        )
      }
//    case class Queries(writes: WriteQueries[S, F, Sortable],
//                       truncate: S[WriteOp],
//                       int1: UUID => S[Sortable],
//                       int2: UUID => S[Sortable],
//                       string1: UUID => S[Sortable],
//                       string2: UUID => S[Sortable],
//                       short1: UUID => S[Sortable],
//                       short2: UUID => S[Sortable],
//                       long1: UUID => S[Sortable],
//                       long2: UUID => S[Sortable],
//                       float1: UUID => S[Sortable],
//                       float2: UUID => S[Sortable],
//                       double1: UUID => S[Sortable],
//                       double2: UUID => S[Sortable],
//                       uuid1: UUID => S[Sortable],
//                       uuid2: UUID => S[Sortable])

      override val queries: (Queries, Queries) = {
        run(tables.toVector.traverse_(delAndCreate))
        (mkQueries(true), mkQueries(false))
      }

    })
}
