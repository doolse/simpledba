package io.doolse.simpledba.cassandra

import java.util.{Date, UUID}

import com.datastax.driver.core.querybuilder.{Assignment, QueryBuilder}
import com.datastax.driver.core.{DataType, Row, TypeCodec}

/**
  * Created by jolz on 21/05/16.
  */
sealed trait CassandraColumn[A] {
  def byName(r: Row, n: String): Option[A]
  val binding: A => AnyRef
  def dataType: DataType
  def assignment(name: String, ex: A, v: A): Assignment = {
    QueryBuilder.set(name, binding(v))
  }
}

case class CassandraCodecColumn[A0, A](dataType: DataType, tt: TypeCodec[A0], from: A0 => A, binding: A => AnyRef) extends CassandraColumn[A] {
  def byName(r: Row, n: String): Option[A] = Option(r.get(n, tt)).map(from)
}

case class OptionalColumn[T](inner: CassandraColumn[T]) extends CassandraColumn[Option[T]] {
  def byName(r: Row, n: String): Option[Option[T]] = Some(inner.byName(r, n))

  val binding: (Option[T]) => AnyRef = _.map(inner.binding).orNull

  def dataType: DataType = inner.dataType

  override def assignment(name: String, exO: Option[T], vO: Option[T]): Assignment = (exO, vO) match {
    case (Some(ex), Some(v)) => inner.assignment(name, ex, v)
    case _ => QueryBuilder.set(name, binding(vO))
  }
}

object CassandraCodecColumn {
  def direct[T](dt: DataType, tc: TypeCodec[T]): CassandraColumn[T] = CassandraCodecColumn[T, T](dt, tc, identity, _.asInstanceOf[AnyRef])
}

object CassandraColumn {
  implicit val longCol : CassandraColumn[Long] = CassandraCodecColumn(DataType.bigint(), TypeCodec.bigint(), Long2long, _.asInstanceOf[AnyRef])
  implicit val intCol : CassandraColumn[Int] = CassandraCodecColumn(DataType.cint(), TypeCodec.cint(), Integer2int, _.asInstanceOf[AnyRef])
  implicit val boolCol : CassandraColumn[Boolean] = CassandraCodecColumn(DataType.cboolean(), TypeCodec.cboolean(), Boolean2boolean, _.asInstanceOf[AnyRef])
  implicit val stringCol : CassandraColumn[String] = CassandraCodecColumn.direct[String](DataType.text(), TypeCodec.varchar())
  implicit val dateCol : CassandraColumn[Date] = CassandraCodecColumn.direct[Date](DataType.timestamp(), TypeCodec.timestamp())
  implicit val uuidCol : CassandraColumn[UUID] = CassandraCodecColumn.direct[UUID](DataType.uuid(), TypeCodec.uuid())

  implicit def optionalCol[A](implicit col: CassandraColumn[A]) : CassandraColumn[Option[A]] = OptionalColumn(col)
}