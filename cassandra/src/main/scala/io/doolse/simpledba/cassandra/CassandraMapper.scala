package io.doolse.simpledba.cassandra

import cats.{Eval, Monad}
import cats.data.ReaderT
import cats.syntax.all._
import com.datastax.driver.core.schemabuilder.{Create, SchemaBuilder}
import com.datastax.driver.core.{DataType, ResultSet, Row}
import fs2._
import fs2.interop.cats._
import fs2.util.Catchable
import io.doolse.simpledba.CatsUtils._
import io.doolse.simpledba.RelationMapper._
import io.doolse.simpledba.cassandra.CassandraIO._
import io.doolse.simpledba.cassandra.CassandraMapper._
import io.doolse.simpledba.{RelationDef, QueryBuilder => _, _}
import shapeless._
import shapeless.ops.hlist.{Diff, Prepend}
import shapeless.ops.record._


/**
  * Created by jolz on 8/05/16.
  */

object CassandraMapper {
  def exactMatch[T](s: Seq[ColumnMapping[CassandraColumn, T, _]]) = s.map(c => CassandraEQ(c.name))

  def valsToBinding(s: Seq[PhysicalValue[CassandraColumn]]) = s.map(pv => pv.atom.binding(pv.v))

  def rowMaterializer(r: Row) = new ColumnMaterialzer[CassandraColumn] {
    def apply[A](name: String, atom: CassandraColumn[A]): A = atom.byName(r, name).get
  }

}

class CassandraMapper(val config: SimpleMapperConfig = defaultMapperConfig) extends RelationMapper[Effect] {

  type ColumnAtom[A] = CassandraColumn[A]
  type MapperConfig = SimpleMapperConfig
  type DDLStatement = CassandraDDL
  type KeyMapperPoly = CassandraKeyMapper.type
  type QueriesPoly = ConvertCassandraQueries.type
}

trait CassandraTable[T] extends KeyBasedTable {
  def writer(name: String): WriteQueries[Effect, T]

  def createDDL(name: String): CassandraDDL

  def pkNames: Seq[String]

  def skNames: Seq[String]
}

trait CassandraTableBuilder[T, CR <: HList, CVL <: HList, PKL, SKL] {
  def apply(mapper: ColumnMapper[T, CR, CVL], lowPriority: Boolean): CassandraTable[T]
}

object CassandraTableBuilder {
  implicit def cassTable[CR <: HList, AllK <: HList, T, PKL, SKL, CVL <: HList, PKV, SKV]
  (implicit
   allColKeys: Keys.Aux[CR, AllK],
   allCols: ColumnsAsSeq[CR, AllK, T, CassandraColumn],
   pkColLookup: ColumnsAsSeq.Aux[CR, PKL, T, CassandraColumn, PKV],
   skColLookup: ColumnsAsSeq.Aux[CR, SKL, T, CassandraColumn, SKV],
   keyVals: ValueExtractor.Aux[CR, CVL, PKL :: SKL :: HNil, PKV :: SKV :: HNil],
   helperB: ColumnListHelperBuilder[CassandraColumn, T, CR, CVL, PKV :: SKV :: HNil])
  = new CassandraTableBuilder[T, CR, CVL, PKL, SKL] {
    def apply(mapper: ColumnMapper[T, CR, CVL], lowPriority: Boolean) = {
      val extractKey = keyVals()
      val helper = helperB(mapper, extractKey)
      val columnsRecord = mapper.columns
      val (columns, _) = allCols(columnsRecord)
      val (pkCols, pkPhys) = pkColLookup(columnsRecord)
      val (skCols, skPhys) = skColLookup(columnsRecord)

      new CassandraTable[T] {
        val skNames = skCols.map(_.name)
        val pkNames = pkCols.map(_.name)

        def priority = skNames.size + pkNames.size + (if (lowPriority) 0 else 1000)

        def createDDL(tableName: String) = {
          val create = SchemaBuilder.createTable(tableName)
          val dts = columns.map(cm => cm.name -> cm.atom.dataType).toMap
          def addCol(f: (String, DataType) => Create)(name: String) =
            f(CassandraIO.escapeReserved(name), dts(name))

          pkNames.map(addCol(create.addPartitionKey))
          skNames.map(addCol(create.addClusteringColumn))
          (dts.keySet -- (pkNames ++ skNames)).toList.map(addCol(create.addColumn))
          (tableName, create)
        }

        def writer(tableName: String) = new WriteQueries[Effect, T] {
          val C = implicitly[Catchable[Effect]]
          val M = implicitly[Monad[Effect]]
          val F = implicitly[Flushable[Effect]]

          val keyEq = exactMatch(pkCols ++ skCols)
          val insertQ = CassandraInsert(tableName, columns.map(_.name))
          val updateQ = CassandraUpdate(tableName, Seq.empty, keyEq)
          val deleteQ = CassandraDelete(tableName, keyEq)

          def keyBindings(key: PKV :: SKV :: HNil) = valsToBinding(pkPhys(key.head) ++ skPhys(key.tail.head))

          def truncate = F.flush(cassWrite(CassandraWriteOperation(CassandraTruncate(tableName), Seq.empty)))

          def deleteWithKey(key: PKV :: SKV :: HNil) =
            cassWrite(CassandraWriteOperation(deleteQ, keyBindings(key)))

          def insertWithVals(vals: Seq[PhysicalValue[CassandraColumn]]) =
            cassWrite(CassandraWriteOperation(insertQ, valsToBinding(vals)))

          def deleteOp(t: T) = deleteWithKey(helper.extractKey(t))

          def insertOp(t: T) = insertWithVals(helper.toPhysicalValues(t))

          def updateOp(existing: T,newValue: T) = {
            helper.changeChecker(existing, newValue).map {
              case Right((oldKey, newKey, vals)) => deleteWithKey(oldKey) ++ insertWithVals(vals)
              case Left((fk, diff)) =>
                val assignments = diff.map(vd => vd.atom.assigner(vd.name, vd.existing, vd.newValue))
                cassWrite(CassandraWriteOperation(updateQ.copy(assignments = assignments.map(_._1)), assignments.map(_._2) ++ keyBindings(fk)))
            } map (s => (true, s)) getOrElse (false, Stream.empty)
          }

        }
      }
    }
  }

}

object CassandraKeyMapper extends Poly1 {

  implicit def byPK[K, T, CR <: HList, KL <: HList, CVL <: HList]
  (implicit ctb: CassandraTableBuilder[T, CR, CVL, KL, HNil])
  = at[(QueryPK[K], RelationDef[T, CR, KL, CVL])] {
    case (q, relation) => (relation.baseName, ctb(relation.mapper, true))
  }

  implicit def queryMulti[K, Cols <: HList, SortCols <: HList,
  T, CR <: HList, KL <: HList, CVL <: HList,
  LeftOverKL <: HList, LeftOverKL2 <: HList, SKL <: HList]
  (implicit
   diff1: Diff.Aux[KL, Cols, LeftOverKL],
   diff2: Diff.Aux[LeftOverKL, SortCols, LeftOverKL2],
   prepend: Prepend.Aux[SortCols, LeftOverKL2, SKL],
   ctb: CassandraTableBuilder[T, CR, CVL, Cols, SKL]
  )
  = at[(QueryMultiple[K, Cols, SortCols], RelationDef[T, CR, KL, CVL])] {
    case (q, relation) => (relation.baseName, ctb(relation.mapper, false))
  }

  implicit def writes[K, RD] = at[(RelationWriter[K], RD)](_ => ())

}

object MapQuery extends Poly2 {
  implicit def pkQuery[K, T, CR <: HList, PKL <: HList, CVL <: HList,
  KL <: HList, SKL <: HList, AllKL <: HList, PKV]
  (implicit
   allColsKeys: Keys.Aux[CR, AllKL],
   pkColsLookup: ColumnsAsSeq.Aux[CR, PKL, T, CassandraColumn, PKV],
   allColsLookup: ColumnsAsSeq.Aux[CR, AllKL, T, CassandraColumn, CVL],
   materializer: MaterializeFromColumns.Aux[CassandraColumn, CR, CVL]
  )
  = at[QueryPK[K], RelationDef[T, CR, PKL, CVL]] { (q, rd) =>
    val columns = rd.columns
    val (allCols, allValsP) = allColsLookup(columns)
    val (pkCols, pkPhysV) = pkColsLookup(columns)
    val pkNames = pkCols.map(_.name)
    def pkMatcher(ct: CassandraTable[T]) = {
      ct.pkNames == pkNames
    }
    QueryCreate[CassandraTable[T], UniqueQuery[Effect, T, PKV]](pkMatcher, q.nameHint, { (table,_) =>
      val materialize = materializer(columns) andThen rd.mapper.fromColumns compose rowMaterializer
      val allNames = allCols.map(_.name)
      val selectAll = CassandraSelect(table, allNames, Seq.empty, Seq.empty, false)
      val select = CassandraSelect(table, allNames, exactMatch(pkCols), Seq.empty, false)

      val rsStream = (s: Stream[Effect, ResultSet]) => s.flatMap(rs => CassandraIO.rowsStream(rs).translate(task2Effect))
      .map(materialize)

      val queryAll = rsStream (
        Stream.eval[Effect, ResultSet] { cassQuery(_.prepareAndBind(selectAll, Seq.empty)) }
      )

      def doQuery(sv: Stream[Effect, PKV]): Stream[Effect, T] = sv.flatMap { v =>
        rsStream(
          Stream.eval[Effect, ResultSet] {
            cassQuery { _.prepareAndBind(select, valsToBinding(pkPhysV(v))) }
          }
        )
      }
      ??? : UniqueQuery[Effect, T, PKV] // (doQuery, queryAll)
    })
  }


  implicit def rangeQuery[K, Cols <: HList, SortCols <: HList, Q, T, RD, CR <: HList, CVL <: HList,
  PKL <: HList, KL <: HList, SKL <: HList, CRK <: HList,
  ColsVals <: HList, SortVals <: HList, SortLen <: Nat]
  (implicit
   allKeys: Keys.Aux[CR, CRK],
   allColsLookup: ColumnsAsSeq.Aux[CR, CRK, T, CassandraColumn, CVL],
   pkColsLookup: ColumnsAsSeq.Aux[CR, Cols, T, CassandraColumn, ColsVals],
   skColsLookup: ColumnsAsSeq.Aux[CR, SortCols, T, CassandraColumn, SortVals],
   materializer: MaterializeFromColumns.Aux[CassandraColumn, CR, CVL]
  )
  = at[QueryMultiple[K, Cols, SortCols], RelationDef[T, CR, PKL, CVL]] { (q, rd) =>

    val columns = rd.columns
    val (pkCols, pkPhysV) = pkColsLookup(columns)
    val (skCols, skPhysV) = skColsLookup(columns)
    val pkNames = pkCols.map(_.name)
    val skNames = skCols.map(_.name)
    def multiMatcher(ct: CassandraTable[T]) = {
      ct.pkNames == pkNames && ct.skNames.startsWith(skNames)
    }
    QueryCreate[CassandraTable[T], RangeQuery[Effect, T, ColsVals, SortVals]](multiMatcher, q.nameHint, { (tn,_) =>
      val oAsc = skCols.map(cm => (cm.name, true))
      val oDesc = oAsc.map(o => (o._1, false))
      val (allCols, _) = allColsLookup(columns)
      val materialize = materializer(columns) andThen rd.mapper.fromColumns
      val baseSelect = CassandraSelect(tn, allCols.map(_.name), exactMatch(pkCols), Seq.empty, false)

      def doQuery(c: ColsVals, lr: RangeValue[SortVals], ur: RangeValue[SortVals], asc: Option[Boolean]): Stream[Effect, T] = {
        Stream.eval[Effect, ResultSet] {
          cassQuery { s =>
            def processOp(op: Option[(SortVals, Seq[String] => CassandraClause)]) = op.map { case (sv, f) =>
              val vals = skPhysV(sv)
              (Seq(f(vals.map(_.name))), vals)
            }.getOrElse(Seq.empty, Seq.empty)

            val (lw, lv) = processOp(lr.fold(CassandraGTE, CassandraGT))
            val (uw, uv) = processOp(ur.fold(CassandraLTE, CassandraLT))
            val ordering = asc.map(a => if (a) oAsc else oDesc).getOrElse(Seq.empty)
            val select = baseSelect.copy(where = baseSelect.where ++ lw ++ uw, ordering = ordering)
            s.prepareAndBind(select, valsToBinding(pkPhysV(c) ++ lv ++ uv))
          }
        }.flatMap(rs => CassandraIO.rowsStream(rs).translate(task2Effect))
          .map(r => materialize(rowMaterializer(r)))
      }
      RangeQuery(None, doQuery)
    })
  }
}

object ConvertCassandraQueries extends QueryFolder[Effect, CassandraDDL, CassandraTable, MapQuery.type] {

  def createWriter[T](v: Vector[(String, CassandraTable[T])]): WriteQueries[Effect, T] = v.map {
    case (name, table) => table.writer(name)
  }.reduce(WriteQueries.combine[Effect, T])

  def createTableDDL[T](s: String, table: CassandraTable[T]): (String, Create) = table.createDDL(s)
}