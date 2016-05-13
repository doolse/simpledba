package io.doolse.simpledba

import cats.Monad
import cats.free.Free
import cats.syntax.{FlatMapOps, FlatMapSyntax}

/**
  * Created by jolz on 4/05/16.
  */

sealed trait ColumnReference
case class ColumnName(name: String) extends ColumnReference
case class ColumnIndex(idx: Int) extends ColumnReference

case class SortColumn(name: ColumnName, ascending: Boolean)

sealed trait ColumnMetaType
case object PartitionKey extends ColumnMetaType
case object SortKey extends ColumnMetaType
case object StandardColumn extends ColumnMetaType

case class ColumnMetadata(name: String, columnType: ColumnMetaType)

sealed trait RelationQuery {
  def isWrite = false
}
sealed trait RelationWriteQuery extends RelationQuery {
  override def isWrite = true
}
case class SelectQuery(table: String, columns: List[ColumnName], keyColumns: List[ColumnName], sortedBy: Option[SortColumn] = None) extends RelationQuery
case class InsertQuery(table: String, columns: List[ColumnName]) extends RelationWriteQuery
case class UpdateQuery(table: String, updatedFields: List[ColumnName], keyColumns: List[ColumnName]) extends RelationWriteQuery
case class DeleteQuery(table: String, keyColumns: List[ColumnName]) extends RelationWriteQuery

abstract class RelationIO[F[_], RSOps[_]] {
  type CT[T]
  trait QP {
    type T
    val v: Option[(T, CT[T])]
    override def toString = v.toString
  }
  type RS
  val rsOps : ResultSetOps[RSOps, CT]
  def query(q: RelationQuery, params: Iterable[QP]) : F[RS]
  def usingResults[A](rs: RS, op: RSOps[A]): F[A]
  def parameter[T0](c: CT[T0], v0: T0) : QP = new QP {
    type T = T0
    val v = Option((v0, c))
  }
}

object RelationIO {
  type Aux[F0[_], RSOps0[_], CT0[_]] = RelationIO[F0, RSOps0] {
    type CT[A] = CT0[A]
  }
}

abstract class ResultSetOps[F[_], CT[_]] {
  def isNull(ref: ColumnReference): F[Boolean]
  def nextResult: F[Boolean]
  def haveMoreResults: F[Boolean]
  def getColumn[T](ref: ColumnReference, ct: CT[T]): F[Option[T]]
}
