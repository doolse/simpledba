package io.doolse.simpledba2.jdbc

import java.sql._

import io.doolse.simpledba2._
import shapeless._

trait JDBCColumn {
  type A

  def sqlType: SQLType

  def nullable: Boolean

  def getByIndex: (Int, ResultSet) => Option[A]

  def bind: (Int, A, Connection, PreparedStatement) => Unit
}

case class JDBCRelation[C[_], T, R <: HList](name: String, sqlMapping: JDBCSQLConfig, all: Columns[C, T, R]) {
  def primaryKey[S <: Symbol](k: Witness.Aux[S])(implicit css: ColumnSubset[R, S :: HNil]): JDBCTable[C, T, R, css.Out] = {
    val (keys, toKey) = all.subset[S :: HNil]
    JDBCTable(name, sqlMapping, all, keys, toKey)
  }

  def primaryKeys[S1 <: Symbol, S2 <: Symbol](k1: Witness.Aux[S1], k2: Witness.Aux[S2])
                                             (implicit css: ColumnSubset[R, S1 :: S2 :: HNil]): JDBCTable[C, T, R, css.Out] = {
    val (keys, toKey) = all.subset[S1 :: S2 :: HNil]
    JDBCTable(name, sqlMapping, all, keys, toKey)
  }
}

case class JDBCTable[C[_], T, R <: HList, K <: HList](name: String, sqlMapping: JDBCSQLConfig, all: Columns[C, T, R],
                                                      keys: Columns[C, K, K], toKey: R => K)



