package io.doolse.simpledba.cassandra

import cats.data.ReaderT
import com.datastax.driver.core.TypeCodec
import fs2.util.Task
import io.doolse.simpledba.RelationMapper
import io.doolse.simpledba.cassandra.CassandraMapper.Effect
import fs2.interop.cats._
import shapeless.{HList, HNil}
import shapeless.ops.hlist.IsHCons

/**
  * Created by jolz on 8/05/16.
  */

object CassandraMapper {
  type Effect[A] = ReaderT[Task, SessionConfig, A]
}

class CassandraMapper extends RelationMapper[Effect, CassandraColumn] {

  implicit def noSortKeyMapper[T, CR <: HList, KL <: HList, CVL <: HList, PKL <: HList]
  : KeyMapper.Aux[T, CR, KL, CVL, PKL, HNil, HNil, HNil, HNil] = ???

}
