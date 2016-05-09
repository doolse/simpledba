package io.doolse.simpledba.dynamodb

import cats.{Id, MonadState}
import cats.data.{Reader, State}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScalarAttributeType}
import io.doolse.simpledba.dynamodb.DynamoDBRelationIO.{Effect, ResultOps}
import io.doolse.simpledba._
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * Created by jolz on 5/05/16.
  */

case class DynamoDBColumn[T](from: AttributeValue => T, to: T => AttributeValue, attributeType: ScalarAttributeType)

case class DynamoDBResultSet(rs: Iterator[Map[String, AttributeValue]], row: Option[Map[String, AttributeValue]])

case class DynamoDBSession(client: AmazonDynamoDBClient)

object DynamoDBRelationIO {
  type Effect[A] = Reader[DynamoDBSession, A]
  type ResultOps[A] = State[DynamoDBResultSet, A]
}

class DynamoDBRelationIO extends RelationIO[Effect, ResultOps] {
  type CT[A] = DynamoDBColumn[A]
  type RS = DynamoDBResultSet

  def qpToAttr[A](cn: (ColumnName, QP)): (String, AttributeValue) =
    cn._1.name -> cn._2.v.map(t => t._2.to(t._1)).getOrElse(new AttributeValue().withNULL(true))

  def query(q: RelationQuery, params: Iterable[QP]): Effect[DynamoDBResultSet] = Reader {
    s => q match {
      case InsertQuery(table, columns) =>
        val valMap = columns.zip(params).map(qpToAttr).toMap
        s.client.putItem(table, valMap.asJava)
        DynamoDBResultSet(Iterator.empty, None)
      case SelectQuery(table, columns, keyColumns, sortedBy) =>
        val keys = keyColumns.zip(params).map(qpToAttr).toMap
        DynamoDBResultSet(Some(s.client.getItem(table, keys.asJava).getItem.asScala.toMap).iterator, None)
    }
  }

  def usingResults[A](rs: DynamoDBResultSet, op: ResultOps[A]): Effect[A] = Reader { _ => op.runA(rs).value }

  val rsOps: ResultSetOps[ResultOps, DynamoDBColumn] = new ResultSetOps[ResultOps, DynamoDBColumn] {
    val MS = MonadState[ResultOps, DynamoDBResultSet]

    import MS._

    def noIndexes = sys.error("Column indexes not supported")

    def noCurrentRow = sys.error("column access with no current row")

    def isNull(ref: ColumnReference): ResultOps[Boolean] = ref match {
      case ColumnName(name) => inspect(_.row.map(_.contains(name)).getOrElse(noCurrentRow))
      case ColumnIndex(idx) => noIndexes
    }

    def haveMoreResults: ResultOps[Boolean] = inspect(_.rs.hasNext)

    def getColumn[T](ref: ColumnReference, ct: DynamoDBColumn[T]): ResultOps[Option[T]] = inspect {
      _.row.map { r =>
        ref match {
          case ColumnName(name) => r.get(name).filterNot(_.isNULL != null).map(ct.from)
          case ColumnIndex(idx) => noIndexes
        }
      } getOrElse noCurrentRow
    }

    def nextResult: ResultOps[Boolean] = State { s: DynamoDBResultSet =>
      val nextRow = if (s.rs.hasNext) Some(s.rs.next()) else None
      (s.copy(row = nextRow), nextRow.isDefined)
    }
  }
}
