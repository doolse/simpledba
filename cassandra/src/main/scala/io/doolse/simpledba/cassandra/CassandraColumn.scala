package io.doolse.simpledba.cassandra

import com.datastax.driver.core.{Row, TypeCodec}

/**
  * Created by jolz on 21/05/16.
  */
sealed trait CassandraColumn[T] {
  def byName(r: Row, n: String): Option[T]
  val binding: T => AnyRef
}

case class CassandraCodecColumn[T0, T](tt: TypeCodec[T0], from: T0 => T, binding: T => AnyRef) extends CassandraColumn[T] {
  def byName(r: Row, n: String): Option[T] = Option(r.get(n, tt)).map(from)
}

object CassandraCodecColumn {
  def direct[T](tc: TypeCodec[T]): CassandraColumn[T] = CassandraCodecColumn[T, T](tc, identity, _.asInstanceOf[AnyRef])
}

object CassandraColumn {
  implicit val longCol : CassandraColumn[Long] = CassandraCodecColumn(TypeCodec.bigint(), Long2long, _.asInstanceOf[AnyRef])
  implicit val intCol : CassandraColumn[Int] = CassandraCodecColumn(TypeCodec.cint(), Integer2int, _.asInstanceOf[AnyRef])
  implicit val boolCol : CassandraColumn[Boolean] = CassandraCodecColumn(TypeCodec.cboolean(), Boolean2boolean, _.asInstanceOf[AnyRef])
  implicit val stringCol : CassandraColumn[String] = CassandraCodecColumn.direct[String](TypeCodec.varchar())
}