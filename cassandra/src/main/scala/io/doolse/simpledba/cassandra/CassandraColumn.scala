package io.doolse.simpledba.cassandra

import com.datastax.driver.core.querybuilder.{Assignment, QueryBuilder}
import com.datastax.driver.core.{DataType, Row, TypeCodec}

/**
  * Created by jolz on 21/05/16.
  */
sealed trait CassandraColumn[T] {
  def byName(r: Row, n: String): Option[T]
  val binding: T => AnyRef
  def dataType: DataType
  def assignment(name: String, ex: T, v: T): Assignment = {
    QueryBuilder.set(name, binding(v))
  }
}

case class CassandraCodecColumn[T0, T](dataType: DataType, tt: TypeCodec[T0], from: T0 => T, binding: T => AnyRef) extends CassandraColumn[T] {
  def byName(r: Row, n: String): Option[T] = Option(r.get(n, tt)).map(from)
}

object CassandraCodecColumn {
  def direct[T](dt: DataType, tc: TypeCodec[T]): CassandraColumn[T] = CassandraCodecColumn[T, T](dt, tc, identity, _.asInstanceOf[AnyRef])
}

object CassandraColumn {
  implicit val longCol : CassandraColumn[Long] = CassandraCodecColumn(DataType.bigint(), TypeCodec.bigint(), Long2long, _.asInstanceOf[AnyRef])
  implicit val intCol : CassandraColumn[Int] = CassandraCodecColumn(DataType.cint(), TypeCodec.cint(), Integer2int, _.asInstanceOf[AnyRef])
  implicit val boolCol : CassandraColumn[Boolean] = CassandraCodecColumn(DataType.cboolean(), TypeCodec.cboolean(), Boolean2boolean, _.asInstanceOf[AnyRef])
  implicit val stringCol : CassandraColumn[String] = CassandraCodecColumn.direct[String](DataType.text(), TypeCodec.varchar())
}