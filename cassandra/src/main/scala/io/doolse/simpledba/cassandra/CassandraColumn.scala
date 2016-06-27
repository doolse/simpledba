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

  def assigner(name: String, ex: A, v: A): (CassandraAssignment, AnyRef) = {
    (SetAssignment(name), binding(v))
  }
}

case class WrappedColumn[S, A](wrapped: CassandraColumn[A], to: S => A, from: A => S) extends CassandraColumn[S] {
  def byName(r: Row, n: String): Option[S] = wrapped.byName(r, n).map(from)

  val binding: (S) => AnyRef = s => wrapped.binding(to(s))

  def dataType: DataType = wrapped.dataType
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

object CassandraColumn {
  def direct[T](dt: DataType, tc: TypeCodec[T]): CassandraColumn[T] = CassandraCodecColumn[T, T](dt, tc, identity, _.asInstanceOf[AnyRef])

  def converted[A0, A](dt: DataType, tc: TypeCodec[A0], c: A0 => A): CassandraColumn[A]
  = CassandraCodecColumn[A0, A](dt, tc, c, _.asInstanceOf[AnyRef])

  implicit val longCol = converted(DataType.bigint(), TypeCodec.bigint(), Long2long)
  implicit val intCol = converted(DataType.cint(), TypeCodec.cint(), Integer2int)
  implicit val boolCol = converted(DataType.cboolean(), TypeCodec.cboolean(), Boolean2boolean)
  implicit val stringCol = direct[String](DataType.text(), TypeCodec.varchar())
  implicit val dateCol = direct[Date](DataType.timestamp(), TypeCodec.timestamp())
  implicit val uuidCol = direct[UUID](DataType.uuid(), TypeCodec.uuid())
  implicit val shortCol = converted(DataType.smallint(), TypeCodec.smallInt(), Short2short)
  implicit val floatCol = converted(DataType.cfloat(), TypeCodec.cfloat(), Float2float)
  implicit val doubleCol = converted(DataType.cdouble(), TypeCodec.cdouble(), Double2double)

  implicit def optionalCol[A](implicit col: CassandraColumn[A]): CassandraColumn[Option[A]] = OptionalColumn(col)
}