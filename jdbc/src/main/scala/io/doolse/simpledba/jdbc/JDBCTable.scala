package io.doolse.simpledba.jdbc

import io.doolse.simpledba._
import shapeless.{::, HList, HNil, Witness}

case class JDBCRelation[C[_] <: JDBCColumn[_], T, R <: HList](
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

trait JDBCTable[C[_] <: JDBCColumn[_]] {
  type Data
  type DataRec <: HList
  type KeyList <: HList
  type KeyNames <: HList

  def name: String
  def toKey: DataRec => KeyList
  implicit def keySubset: ColumnSubsetBuilder.Aux[DataRec, KeyNames, KeyList]
  def keyNames                                           = new Cols[KeyNames]
  lazy val keyColumns: ColumnSubset[C, DataRec, KeyList] = cols(keyNames)

  def allColumns: Columns[C, Data, DataRec]

  def definition: TableDefinition =
    TableDefinition(
      name,
      allColumns.columns.map(NamedColumn.apply),
      keyColumns.columns.map(_._1)
    )

  def cols[Names <: HList](
      c: Cols[Names]
  )(implicit ss: ColumnSubsetBuilder[DataRec, Names]): ColumnSubset[C, DataRec, ss.Out] =
    allColumns.subset[Names]

  def subset[Names <: HList](
      c: Cols[Names]
  )(implicit ss: ColumnSubsetBuilder[DataRec, Names]): TableColumns =
    TableColumns(name, allColumns.subset[Names].columns.map(NamedColumn.apply))
}

object JDBCTable {
  type Aux[C[_] <: JDBCColumn[_], T, R, K, KeyN] = JDBCTable[C] {
    type Data     = T
    type DataRec  = R
    type KeyNames = KeyN
    type KeyList  = K
  }

  type TableRecord[C[_] <: JDBCColumn[_], R] = JDBCTable[C] {
    type DataRec = R
  }

  def apply[C[_] <: JDBCColumn[_], T, R <: HList, K <: HList, KeyN <: HList](
      tableName: String,
      all: Columns[C, T, R],
      keys: ColumnSubsetBuilder.Aux[R, KeyN, K],
      tkey: R => K
  ) = new JDBCTable[C] {
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
