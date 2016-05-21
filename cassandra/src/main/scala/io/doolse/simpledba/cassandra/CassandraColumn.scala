package io.doolse.simpledba.cassandra

import com.datastax.driver.core.{Row, TypeCodec}

/**
  * Created by jolz on 21/05/16.
  */
sealed trait CassandraColumn[T] {
  def byName(r: Row, n: String): Option[T]

  def byIndex(r: Row, idx: Int): Option[T]

  val binding: T => AnyRef
}

case class CassandraCodecColumn[T0, T](tt: TypeCodec[T0], from: T0 => T, binding: T => AnyRef) extends CassandraColumn[T] {
  def byName(r: Row, n: String): Option[T] = Option(r.get(n, tt)).map(from)

  def byIndex(r: Row, idx: Int): Option[T] = Option(r.get(idx, tt)).map(from)
}

object CassandraCodecColumn {
  def direct[T](tc: TypeCodec[T]): CassandraColumn[T] = CassandraCodecColumn[T, T](tc, identity, _.asInstanceOf[AnyRef])
}