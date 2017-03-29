package io.doolse.simpledba.jdbc

import java.sql.ResultSet

import cats.data.{Kleisli, ReaderT}
import cats.{Monad, Now}
import cats.syntax.all._
import fs2._
import fs2.interop.cats._
import fs2.util.Catchable
import io.doolse.simpledba.CatsUtils._
import io.doolse.simpledba.RelationMapper._
import io.doolse.simpledba.{RangeValue, _}
import io.doolse.simpledba.jdbc._
import shapeless._
import shapeless.ops.hlist.{Mapper, ToList}
import shapeless.ops.record.Keys

/**
  * Created by jolz on 12/03/17.
  */


class JDBCMapper(val config: SimpleMapperConfig = defaultMapperConfig) extends RelationMapper[Effect] {
  type MapperConfig = SimpleMapperConfig
  type DDLStatement = JDBCDDL
  type ColumnAtom[A] = JDBCColumn[A]
  type KeyMapperPoly = JDBCKeyMapper.type
  type QueriesPoly = JDBCQueries.type
}

case class TableWithIndexes(table: JDBCCreateTable)

trait JDBCTableBuilder[T, CR <: HList, PKL <: HList] {
  def apply(relation: RelationDef[T, CR, PKL, _]): JDBCCreateTable
}

object JDBCTableBuilder {
  implicit def tableForRelation[T, CR <: HList, CVL <: HList, PKL <: HList, AllK <: HList](implicit
                                allColKeys: Keys.Aux[CR, AllK],
                                allCols: ColumnsAsSeq[CR, AllK, T, JDBCColumn],
                                pkCols: ColumnsAsSeq[CR, PKL, T, JDBCColumn]) : JDBCTableBuilder[T, CR, PKL]
  = (relation: RelationDef[T, CR, PKL, _]) => {
    val (cols, _) = allCols(relation.columns)
    val (pk, _) = pkCols(relation.columns)
    JDBCCreateTable(relation.baseName, cols.map(c => (c.name, c.atom.columnType)), pk.map(_.name))
  }
}

object JDBCQueryMap extends Poly1 {
  implicit def pkQuery[K, T, CR <: HList, PKL <: HList, CVL <: HList,
  KL <: HList, SKL <: HList, AllKL <: HList, PKV]
  (implicit
   allColsKeys: Keys.Aux[CR, AllKL],
   pkColsLookup: ColumnsAsSeq.Aux[CR, PKL, T, JDBCColumn, PKV],
   allColsLookup: ColumnsAsSeq.Aux[CR, AllKL, T, JDBCColumn, CVL],
   materializer: MaterializeFromColumns.Aux[JDBCColumn, CR, CVL]
  )
  = at[(QueryPK[K], RelationDef[T, CR, PKL, CVL])] { case (q, rd) =>
    val columns = rd.columns
    val table = rd.baseName
    val (allCols, allValsP) = allColsLookup(columns)
    val (pkCols, pkPhysV) = pkColsLookup(columns)
    val pkNames = pkCols.map(_.name)
    val materialize = materializer(columns) andThen rd.mapper.fromColumns
    val allNames = allCols.map(_.name)
    val selectAll = JDBCSelect(table, allNames, Seq.empty, Seq.empty, false)
    val select = JDBCSelect(table, allNames, JDBCPreparedQuery.exactMatch(pkCols), Seq.empty, false)

    def rsStream(s: Stream[Effect, ResultSet]) = for {
      c <- Stream.eval[Effect, JDBCSession](Kleisli.ask[JDBCWriter, JDBCSession])
      rs <- s.flatMap(JDBCIO.rowsStream)
    } yield materialize(JDBCIO.rowMaterializer(c, rs))

    val queryAll = rsStream(
      Stream.eval(JDBCIO.sessionIO(_.prepare(selectAll).map(_.executeQuery())))
    )

    def doQuery(sv: Stream[Effect, PKV]): Stream[Effect, T] = sv.flatMap { v =>
      rsStream(
        Stream.eval[Effect, ResultSet](JDBCIO.sessionIO(_.execQuery(select, pkPhysV(v))))
      )
    }

    UniqueQuery[Effect, T, PKV](doQuery, queryAll)
  }


  implicit def rangeQuery[K, Cols <: HList, SortCols <: HList, Q, T, RD, CR <: HList, CVL <: HList,
  PKL <: HList, KL <: HList, SKL <: HList, CRK <: HList,
  ColsVals <: HList, SortVals <: HList]
  (implicit
   allKeys: Keys.Aux[CR, CRK],
   allColsLookup: ColumnsAsSeq.Aux[CR, CRK, T, JDBCColumn, CVL],
   pkColsLookup: ColumnsAsSeq.Aux[CR, Cols, T, JDBCColumn, ColsVals],
   skColsLookup: ColumnsAsSeq.Aux[CR, SortCols, T, JDBCColumn, SortVals],
   materializer: MaterializeFromColumns.Aux[JDBCColumn, CR, CVL]
  )
  = at[(QueryMultiple[K, Cols, SortCols], RelationDef[T, CR, PKL, CVL])] { case (q, rd) =>
    val columns = rd.columns
    val tn = rd.baseName
    val (pkCols, pkPhysV) = pkColsLookup(columns)
    val (skCols, skPhysV) = skColsLookup(columns)
    val pkNames = pkCols.map(_.name)
    val skNames = skCols.map(_.name)

    val oAsc = skCols.map(cm => (cm.name, true))
    val oDesc = oAsc.map(o => (o._1, false))
    val (allCols, _) = allColsLookup(columns)
    val materialize = materializer(columns) andThen rd.mapper.fromColumns
    val baseSelect = JDBCSelect(tn, allCols.map(_.name), JDBCPreparedQuery.exactMatch(pkCols), Seq.empty, false)

    def doQuery(c: ColsVals, lr: RangeValue[SortVals], ur: RangeValue[SortVals], asc: Option[Boolean]): Stream[Effect, T] = for {
      sess <- Stream.eval[Effect, JDBCSession](ReaderT.ask)
      rs <- Stream.eval[Effect, ResultSet] { JDBCIO.sessionIO { s =>
          def processOp(op: Option[(SortVals, String => JDBCWhereClause)]) = op.map { case (sv, f) =>
            val vals = skPhysV(sv)
            (vals.map(v => f(v.name)), vals)
          }.getOrElse(Seq.empty, Seq.empty)

          val (lw, lv) = processOp(lr.fold(GTE, GT))
          val (uw, uv) = processOp(ur.fold(LTE, LT))
          val ordering = asc.map(a => if (a) oAsc else oDesc).getOrElse(Seq.empty)
          val select = baseSelect.copy(where = baseSelect.where ++ lw ++ uw, ordering = ordering)
          s.execQuery(select, pkPhysV(c) ++ lv ++ uv)
        }
      }.flatMap(rs => JDBCIO.rowsStream(rs))
    } yield materialize(JDBCIO.rowMaterializer(sess, rs))
    RangeQuery(None, doQuery)
  }

  implicit def writes[K, T, CR <: HList, PKL <: HList, CVL <: HList, PKV, AllK <: HList]
  (implicit
   allColKeys: Keys.Aux[CR, AllK],
   allCols: ColumnsAsSeq[CR, AllK, T, JDBCColumn],
   pkColLookup: ColumnsAsSeq.Aux[CR, PKL, T, JDBCColumn, PKV],
   keyVals: ValueExtractor.Aux[CR, CVL, PKL :: HNil, PKV :: HNil],
   helperB: ColumnListHelperBuilder[JDBCColumn, T, CR, CVL, PKV :: HNil]
  )
  = at[(RelationWriter[K], RelationDef[T, CR, PKL, CVL])] { case (_, rd) =>
    val tableName = rd.baseName
    val mapper = rd.mapper
    val columnsRecord = mapper.columns
    val (columns, _) = allCols(columnsRecord)
    val (pkCols, pkPhys) = pkColLookup(columnsRecord)
    val extractKey = keyVals()
    val helper = helperB(mapper, extractKey)

    new WriteQueries[Effect, T] {
      val C = implicitly[Catchable[Effect]]
      val M = implicitly[Monad[Effect]]
      val keyEq = JDBCPreparedQuery.exactMatch(pkCols)
      val updateQ = JDBCUpdate(tableName, Seq.empty, keyEq)
      val deleteQ = JDBCDelete(tableName, keyEq)
      val insertQ = JDBCInsert(tableName, columns.map(_.name))

      def keyBindings(key: PKV :: HNil) = pkPhys(key.head)

      def insert(t: T) = JDBCIO.write(JDBCWrite(insertQ, helper.toPhysicalValues(t)))

//      def insertOperation(t: T): Stream[Effect, WriteOperation] = Stream()

      def truncate = JDBCIO.sessionIO(_.execWrite(JDBCTruncate(tableName), Seq.empty).map(_ => ()))

      def delete(t: T) = deleteWithKey(helper.extractKey(t))

      def deleteWithKey(key: PKV :: HNil) = JDBCIO.write(JDBCWrite(deleteQ, keyBindings(key)))

//      def bulkDelete(l: Stream[Effect, T]): Effect[Unit] = l.evalMap { t =>
//        ReaderT { s: JDBCSession => deleteWithKey(helper.extractKey(t), s) }
//      } run

      def update(existing: T, newValue: T) = {
        helper.changeChecker(existing, newValue).map {
          case Right((oldKey, newKey, vals)) => deleteWithKey(oldKey) >> JDBCIO.write(JDBCWrite(insertQ, vals))
          case Left((fk, diff)) =>
            val bVals = diff.map(vd => PhysicalValue(vd.name, vd.atom, vd.newValue)) ++ keyBindings(fk)
            JDBCIO.write(JDBCWrite(updateQ.copy(assignments = diff.map(_.name)), bVals))
        } map (_.map(_ => true)) getOrElse M.pure(false)
      }
    }: WriteQueries[Effect, T]
  }
}

object JDBCQueries extends Poly3 {
  implicit def convertAll[Q <: HList, Tables <: HList, QOUT <: HList, OutTables <: HList, Config]
  (implicit
   qmap: Mapper.Aux[JDBCQueryMap.type, Q, QOUT],
   toList: ToList[Tables, TableWithIndexes]
  )
  = at[Q, Tables, Config] {
    (q, tables, config) =>
      val distinctTables = toList(tables).map(_.table).distinct
      BuiltQueries[QOUT, JDBCDDL](qmap(q), Now(distinctTables))
  }
}

object JDBCKeyMapper extends Poly1 {

  implicit def byPK[K, T, CR <: HList, KL <: HList, CVL <: HList]
  (implicit tableBuilder: JDBCTableBuilder[T, CR, KL]) = at[(QueryPK[K], RelationDef[T, CR, KL, CVL])] {
    case (q, relation) => TableWithIndexes(tableBuilder(relation))
  }

  implicit def queryMulti[K, Cols <: HList, SortCols <: HList, T, CR <: HList, KL <: HList, CVL <: HList]
  (implicit tableBuilder: JDBCTableBuilder[T, CR, KL])
  = at[(QueryMultiple[K, Cols, SortCols], RelationDef[T, CR, KL, CVL])] {
    case (q, relation) => TableWithIndexes(tableBuilder(relation))
  }

  implicit def writes[K, T, CR <: HList, KL <: HList, CVL <: HList, AllK <: HList]
  (implicit tableBuilder: JDBCTableBuilder[T, CR, KL])
  = at[(RelationWriter[K], RelationDef[T, CR, KL, CVL])] {
    case (q, relation) => TableWithIndexes(tableBuilder(relation))
  }

}
