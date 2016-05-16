package io.doolse.simpledba.cassandra

import com.datastax.driver.core.TypeCodec

/**
  * Created by jolz on 8/05/16.
  */
class CassandraMapper {

  implicit val longCol : CassandraColumn[Long] = CassandraCodecColumn(TypeCodec.bigint(), Long2long, _.asInstanceOf[AnyRef])
  implicit val boolCol : CassandraColumn[Boolean] = CassandraCodecColumn(TypeCodec.cboolean(), Boolean2boolean, _.asInstanceOf[AnyRef])
  implicit val stringCol  = CassandraCodecColumn.direct[String](TypeCodec.varchar())

}
