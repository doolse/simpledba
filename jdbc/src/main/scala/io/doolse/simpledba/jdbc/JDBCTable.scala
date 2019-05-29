package io.doolse.simpledba.jdbc

import io.doolse.simpledba._
import shapeless.{::, HList, HNil, Witness}

case class JDBCRelation[C[_] <: JDBCColumn, T, R <: HList](
    name: String,
    all: Columns[C, T, R]
) {
  def edit[A](k: Witness, col: C[A])(
      implicit css: ColumnSubsetBuilder.Aux[R, k.T :: HNil, A :: HNil]
  ): JDBCRelation[C, T, R] = {
    val (names, _) = css.apply()

    JDBCRelation(name, all.copy(all.columns.map {
      case (cn, c) if names.contains(cn) => (cn, col)
      case o                             => o
    }))
  }

  def key(k: Witness)(
      implicit css: ColumnSubsetBuilder[R, k.T :: HNil]
  ): JDBCTable.Aux[C, T, R, css.Out, k.T :: HNil] = {
    val toKey = all.subset[k.T :: HNil].from
    JDBCTable(name, all, css, toKey)
  }

  def keys[K <: HList](
      k: Cols[K]
  )(implicit css: ColumnSubsetBuilder[R, K]): JDBCTable.Aux[C, T, R, css.Out, K] = {
    val toKey = all.subset[K].from
    JDBCTable(name, all, css, toKey)
  }
}

trait JDBCTable {
  type C[A] <: JDBCColumn
  type Data
  type DataRec <: HList
  type KeyList <: HList
  type KeyNames <: HList

  def name: String
  def toKey: DataRec => KeyList
  implicit def keySubset: ColumnSubsetBuilder.Aux[DataRec, KeyNames, KeyList]
  def keyNames                                                    = new Cols[KeyNames]
  lazy val keyColumns: ColumnSubset[C, DataRec, KeyList] = cols(keyNames)
  def allColumns: Columns[C, Data, DataRec]

  def definition: TableDefinition =
    TableDefinition(
      name,
      allColumns.columns.map(NamedColumn.apply[C]),
      keyColumns.columns.map(_._1)
    )

  def cols[Names <: HList](
      c: Cols[Names]
  )(implicit ss: ColumnSubsetBuilder[DataRec, Names]): ColumnSubset[C, DataRec, ss.Out] =
    allColumns.subset[Names]

  def subset[Names <: HList](
      c: Cols[Names]
  )(implicit ss: ColumnSubsetBuilder[DataRec, Names]): TableColumns =
    TableColumns(name, allColumns.subset[Names].columns.map(NamedColumn.apply[C]))
}

object JDBCTable {
  type Aux[C2[_] <: JDBCColumn, T, R, K, KeyN] = JDBCTable {
    type C[A]     = C2[A]
    type Data     = T
    type DataRec  = R
    type KeyNames = KeyN
    type KeyList  = K
  }

  type TableRecord[C2[_] <: JDBCColumn, R] = JDBCTable {
    type C[A]    = C2[A]
    type DataRec = R
  }

  def apply[C2[_] <: JDBCColumn, T, R <: HList, K <: HList, KeyN <: HList](
      tableName: String,
      all: Columns[C2, T, R],
      keys: ColumnSubsetBuilder.Aux[R, KeyN, K],
      tkey: R => K
  ): Aux[C2, T, R, K, KeyN] = new JDBCTable {
    override type C[A]     = C2[A]
    override type Data     = T
    override type DataRec  = R
    override type KeyList  = K
    override type KeyNames = KeyN

    override def name: String = tableName

    override def toKey = tkey

    override def allColumns = all

    override implicit def keySubset: ColumnSubsetBuilder.Aux[R, KeyN, K] = keys
  }
}
