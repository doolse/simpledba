package io.doolse.simpledba.jdbc

import cats.data.Kleisli
import fs2.{Pipe, Stream}
import io.doolse.simpledba._
import io.doolse.simpledba.jdbc.JDBCQueries._
import shapeless.{::, HList, HNil, SingletonProductArgs, Witness}
import cats.syntax.apply._

case class JDBCRelation[C[_] <: JDBCColumn, T, R <: HList](name: String, sqlMapping: JDBCConfig, all: Columns[C, T, R]) extends SingletonProductArgs {
  def key(k: Witness)(implicit css: ColumnSubsetBuilder[R, k.T :: HNil]): JDBCTable[C, T, R, css.Out] = {
    val (keys, toKey) = all.subset[k.T :: HNil]
    JDBCTable(name, sqlMapping, all, keys, toKey)
  }

  def key(k1: Witness, k2: Witness)(implicit css: ColumnSubsetBuilder[R, k1.T :: k2.T :: HNil]): JDBCTable[C, T, R, css.Out] = {
    val (keys, toKey) = all.subset[k1.T :: k2.T :: HNil]
    JDBCTable(name, sqlMapping, all, keys, toKey)
  }

  def keysProduct[K <: HList](k: K)
                             (implicit css: ColumnSubsetBuilder[R, K]): JDBCTable[C, T, R, css.Out] = {
    val (keys, toKey) = all.subset[K]
    JDBCTable(name, sqlMapping, all, keys, toKey)
  }
}

case class JDBCWriteOp(q: JDBCPreparedQuery, config: JDBCConfig, binder: BindFunc[Seq[BindLog]]) extends WriteOp

case class JDBCTable[C[_] <: JDBCColumn, T, R <: HList, K <: HList]
(name: String, config: JDBCConfig, all: Columns[C, T, R],
 keys: ColumnSubset[C, R, K, K], toKey: R => K) {
  def col(w: Witness)(implicit ss: ColumnSubsetBuilder[R, w.T :: HNil]): ColumnSubset[C, R, ss.Out, ss.Out] =
    all.subset._1

  def writes = new WriteQueries[JDBCIO, T] {

    def truncate = Stream(writeOp(JDBCTruncate(name)))

    val insertQuery = JDBCInsert(name, all.columns.map(JDBCColumnBinding.apply[C]))

    override def insertAll: Pipe[JDBCIO, T, WriteOp] = _.map {
      t => JDBCWriteOp(insertQuery, config, bindCols(all, all.iso.to(t))
        .map(v => Seq(UpdateBinding(v))))
    }

    override def updateAll: Pipe[JDBCIO, (T, T), WriteOp] = _.flatMap {
      case (o, n) =>
        val oldKey = toKey(all.iso.to(o))
        val newRec = all.iso.to(n)
        val keyVal = toKey(newRec)
        if (oldKey != keyVal) {
          Stream(deleteWriteOp(oldKey)) ++ insert(n)
        }
        else {
          val (whereClauses,bind) = colsEQ(keys).bind(keyVal)
          val binder = for {
            updateVals <- bindCols(all, newRec)
            wc <- bind
          } yield Seq(UpdateBinding(updateVals)) ++ wc
          Stream(JDBCWriteOp(JDBCUpdate(name, all.columns.map(JDBCColumnBinding.apply[C]),
            whereClauses), config, binder))
        }
    }

    override def deleteAll: Pipe[JDBCIO, T, WriteOp] = _.map(t => deleteWriteOp(toKey(all.iso.to(t))))

  }

  private def deleteWriteOp(k: K): JDBCWriteOp = {
    val (whereClauses,bindVals) = colsEQ(keys).bind(k)
    JDBCWriteOp(JDBCDelete(name, whereClauses), config,
      bindVals.map(_.toList))
  }

  def query = new QueryBuilder[C, T, R, K, Unit, BindNone.type, T, R](this, all, BindNone, Seq.empty)

  def byColumn[K2](column: Witness)(implicit cs: ColumnSubsetBuilder.Aux[R, column.T :: HNil, K],
                                 c: AutoConvert[K2, K]) : K2 => Stream[JDBCIO, T] =
    query.whereEQ(col(column)).build[K2]

  def byPK[K2](implicit c: AutoConvert[K2, K]) : K2 => Stream[JDBCIO, T] = query.whereEQ(keys).build[K2]

  def allRows: Stream[JDBCIO, T] = query.build[Unit].apply()

  private def writeOp(q: JDBCPreparedQuery) =
    JDBCWriteOp(q, config, Kleisli.pure(Seq.empty))

  def createTable: JDBCWriteOp = writeOp(JDBCCreateTable(name, all.columns.map(JDBCColumnBinding.apply[C]),
    keys.columns.map(JDBCColumnBinding.apply[C])))

  def dropTable: JDBCWriteOp = writeOp(JDBCDropTable(name))
}
