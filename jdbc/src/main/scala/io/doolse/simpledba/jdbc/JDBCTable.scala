package io.doolse.simpledba.jdbc

import fs2.{Pipe, Stream}
import io.doolse.simpledba._
import io.doolse.simpledba.jdbc.JDBCQueries._
import shapeless.{::, HList, HNil, SingletonProductArgs, Witness}

case class JDBCRelation[C[_] <: JDBCColumn, T, R <: HList](
    name: String,
    sqlMapping: JDBCConfig,
    all: Columns[C, T, R]
) {
  def edit[A](k: Witness, col: C[A])(
      implicit css: ColumnSubsetBuilder.Aux[R, k.T :: HNil, A :: HNil]
  ): JDBCRelation[C, T, R] = {
    val (names, _) = css.apply()

    JDBCRelation(name, sqlMapping, all.copy(all.columns.map {
      case (cn, c) if names.contains(cn) => (cn, col)
      case o                             => o
    }))
  }

  def key(k: Witness)(
      implicit css: ColumnSubsetBuilder[R, k.T :: HNil]
  ): JDBCTable.Aux[C, T, R, css.Out, k.T :: HNil] = {
    val (keys, toKey) = all.subset[k.T :: HNil]
    JDBCTable(name, sqlMapping, all, css, toKey)
  }

  def keys[K <: HList](
      k: Cols[K]
  )(implicit css: ColumnSubsetBuilder[R, K]): JDBCTable.Aux[C, T, R, css.Out, K] = {
    val (keys, toKey) = all.subset[K]
    JDBCTable(name, sqlMapping, all, css, toKey)
  }
}

trait JDBCTable[C[_] <: JDBCColumn] {
  type Data
  type DataRec <: HList
  type KeyList <: HList
  type KeyNames <: HList

  def name: String
  def config: JDBCConfig
  def toKey: DataRec => KeyList
  implicit def keySubset: ColumnSubsetBuilder.Aux[DataRec, KeyNames, KeyList]
  def keyNames                                                    = new Cols[KeyNames]
  lazy val keyColumns: ColumnSubset[C, DataRec, KeyList, KeyList] = cols(keyNames)
  def allColumns: Columns[C, Data, DataRec]

  def writes: WriteQueries[JDBCIO, Data] = new WriteQueries[JDBCIO, Data] {

    override def insertAll: Pipe[JDBCIO, Data, WriteOp] = _.map { t =>
      val columnBinders = bindValues(allColumns, allColumns.iso.to(t))
      val insertQuery   = JDBCInsert(name, columnExpressions(columnBinders))
      JDBCWriteOp(
        insertQuery,
        config,
        bindParameters(columnBinders.columns.map(_._1._1)).map(v => Seq(ValueLog(v)))
      )
    }

    override def updateAll: Pipe[JDBCIO, (Data, Data), WriteOp] = _.flatMap {
      case (o, n) =>
        val oldRec = allColumns.iso.to(o)
        val oldKey = toKey(oldRec)
        val newRec = allColumns.iso.to(n)
        val keyVal = toKey(newRec)
        if (oldKey != keyVal) {
          Stream(deleteWriteOp(oldKey)) ++ insert(n)
        } else {
          val (whereClauses, bind) = colsOp(BinOp.EQ, keyColumns).bind(keyVal)
          val updateColumns        = bindUpdate(allColumns, oldRec, newRec)
          if (updateColumns.columns.isEmpty) Stream.empty
          else {
            val binder = for {
              updateVals <- bindParameters(updateColumns.columns.map(_._1._1))
              wc         <- bind
            } yield Seq(ValueLog(updateVals)) ++ wc
            Stream(
              JDBCWriteOp(
                JDBCUpdate(name, columnExpressions(updateColumns), whereClauses),
                config,
                binder
              )
            )
          }
        }
    }

    override def deleteAll: Pipe[JDBCIO, Data, WriteOp] =
      _.map(t => deleteWriteOp(toKey(allColumns.iso.to(t))))

  }

  private def deleteWriteOp(k: KeyList): JDBCWriteOp = {
    val (whereClauses, bindVals) = colsOp(BinOp.EQ, keyColumns).bind(k)
    JDBCWriteOp(JDBCDelete(name, whereClauses), config, bindVals.map(_.toList))
  }

  def select =
    new QueryBuilder[C, DataRec, BindNone.type, HNil, HNil](
      this,
      ColumnRecord.empty,
      identity,
      BindNone,
      Seq.empty
    )

  def select[ColNames <: HList, Out <: HList](
      cols: Cols[ColNames]
  )(implicit css: ColumnSubsetBuilder.Aux[DataRec, ColNames, Out]) =
    new QueryBuilder[C, DataRec, BindNone.type, HNil, HNil](
      this,
      ColumnRecord.empty,
      identity,
      BindNone,
      Seq.empty
    ).cols[Out, Out, ColNames](cols)

  def delete = new DeleteBuilder[C, DataRec, BindNone.type](this, BindNone)

  def query =
    new QueryBuilder[C, DataRec, BindNone.type, DataRec, Data](
      this,
      toProjection(allColumns),
      allColumns.iso.from,
      BindNone,
      Seq.empty
    )

  def byPK[K2](implicit c: AutoConvert[K2, KeyList]): K2 => Stream[JDBCIO, Data] = {
    query.where(new Cols[KeyNames], BinOp.EQ).build[K2]
  }

  def allRows: Stream[JDBCIO, Data] = query.build[Unit].apply()

  def definition: TableDefinition =
    TableDefinition(
      name,
      allColumns.columns.map(NamedColumn.apply[C]),
      keyColumns.columns.map(_._1)
    )

  def cols[Names <: HList](
      c: Cols[Names]
  )(implicit ss: ColumnSubsetBuilder[DataRec, Names]): ColumnSubset[C, DataRec, ss.Out, ss.Out] =
    allColumns.subset[Names]._1

  def subset[Names <: HList](
      c: Cols[Names]
  )(implicit ss: ColumnSubsetBuilder[DataRec, Names]): TableColumns =
    TableColumns(name, allColumns.subset[Names]._1.columns.map(NamedColumn.apply[C]))
}

object JDBCTable {
  type Aux[C[_] <: JDBCColumn, T, R, K, KeyN] = JDBCTable[C] {
    type Data     = T
    type DataRec  = R
    type KeyNames = KeyN
    type KeyList  = K
  }

  type TableRecord[C[_] <: JDBCColumn, R] = JDBCTable[C] {
    type DataRec = R
  }

  def apply[C[_] <: JDBCColumn, T, R <: HList, K <: HList, KeyN <: HList](
      tableName: String,
      jdbcConfig: JDBCConfig,
      all: Columns[C, T, R],
      keys: ColumnSubsetBuilder.Aux[R, KeyN, K],
      tkey: R => K
  ): Aux[C, T, R, K, KeyN] = new JDBCTable[C] {
    override type Data     = T
    override type DataRec  = R
    override type KeyList  = K
    override type KeyNames = KeyN

    override def name: String = tableName

    override def config: JDBCConfig = jdbcConfig

    override def toKey = tkey

    override def allColumns = all

    override implicit def keySubset: ColumnSubsetBuilder.Aux[R, KeyN, K] = keys
  }
}
