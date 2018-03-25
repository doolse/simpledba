package io.doolse.simpledba2.jdbc

import java.sql._

import cats.effect.IO
import fs2.{Pipe, Stream}
import io.doolse.simpledba2._
import io.doolse.simpledba2.jdbc.JDBCQueries._
import shapeless._

trait JDBCColumn {
  type A

  def sqlType: SQLType

  def nullable: Boolean

  def getByIndex: (Int, ResultSet) => Option[A]

  def bind: (Int, A, Connection, PreparedStatement) => Unit
}

case class JDBCRelation[C[_] <: JDBCColumn, T, R <: HList](name: String, sqlMapping: JDBCSQLDialect, all: Columns[C, T, R]) extends SingletonProductArgs {
  def primaryKey(k: Witness)(implicit css: ColumnSubsetBuilder[R, k.T :: HNil]): JDBCTable[C, T, R, css.Out] = {
    val (keys, toKey) = all.subset[k.T :: HNil]
    JDBCTable(name, sqlMapping, all, keys, toKey)
  }

  def primaryKey(k1: Witness, k2: Witness)(implicit css: ColumnSubsetBuilder[R, k1.T :: k2.T :: HNil]): JDBCTable[C, T, R, css.Out] = {
    val (keys, toKey) = all.subset[k1.T :: k2.T :: HNil]
    JDBCTable(name, sqlMapping, all, keys, toKey)
  }

  def primaryKeysProduct[K <: HList](k: K)
                                             (implicit css: ColumnSubsetBuilder[R, K]): JDBCTable[C, T, R, css.Out] = {
    val (keys, toKey) = all.subset[K]
    JDBCTable(name, sqlMapping, all, keys, toKey)
  }
}

case class JDBCWriteOp(query: JDBCPreparedQuery, config: JDBCSQLDialect, bind: (Connection, PreparedStatement) => IO[Unit]) extends WriteOp

case class JDBCTable[C[_] <: JDBCColumn, T, R <: HList, K <: HList](name: String, sqlMapping: JDBCSQLDialect, all: Columns[C, T, R],
                                                      keys: ColumnSubset[C, R, K, K], toKey: R => K)
{
  def col(w: Witness)(implicit ss: ColumnSubsetBuilder[R, w.T :: HNil]) : ColumnSubset[C, R, ss.Out, ss.Out] =
    all.subset._1

  def writes = new WriteQueries[JDBCIO, T] {

    val insertQuery = JDBCInsert(name, all.columns.map(_._1))

    override def insertAll: Pipe[JDBCIO, T, WriteOp] = _.map {
      t => JDBCWriteOp(insertQuery, sqlMapping, runBind(bindUnsafe(all.columns, all.iso.to(t))))
    }

    override def updateAll: Pipe[JDBCIO, (T, T), WriteOp] = _.flatMap {
      case (o,n) =>
        val oldKey = toKey(all.iso.to(o))
        val newRec = all.iso.to(n)
        val keyVal = toKey(newRec)
        if (oldKey != keyVal)
        {
          Stream(deleteWriteOp(oldKey)) ++ insert(n)
        }
        else {
          val updateVals = bindUnsafe(all.columns, newRec)
          val whereClause = colsEQ(keys)
          Stream(JDBCWriteOp(JDBCUpdate(name, all.columns.map(_._1),
            whereClause.clauses), sqlMapping, runBind(updateVals.flatMap(_ => whereClause.bind(keyVal)))))
        }
    }

    override def deleteAll: Pipe[JDBCIO, T, WriteOp] = _.map(t => deleteWriteOp(toKey(all.iso.to(t))))

  }

  private def deleteWriteOp(k: K): JDBCWriteOp = {
    val whereClause = colsEQ(keys)
    JDBCWriteOp(JDBCDelete(name, whereClause.clauses), sqlMapping, runBind(whereClause.bind(k)))
  }

  def query = new QueryBuilder[C, T, R, K, Unit, T, R](this, all, Bindable.empty, Seq.empty)

  def allRows : Stream[JDBCIO, T] = query.build[Unit].find.apply(Stream.emit(()))

}



