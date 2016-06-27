package io.doolse.simpledba.cassandra

import java.util.UUID

import cats.{Applicative, Eval}
import cats.data.{ReaderT, Xor}
import cats.syntax.all._
import com.datastax.driver.core.querybuilder.{Select, _}
import com.datastax.driver.core.schemabuilder.{Create, SchemaBuilder}
import com.datastax.driver.core.{DataType, ResultSet, Row, Statement}
import fs2._
import fs2.interop.cats._
import fs2.util.{Catchable, Task}
import io.doolse.simpledba.cassandra.CassandraMapper._
import io.doolse.simpledba.cassandra.CassandraSession._
import io.doolse.simpledba.{RelationDef, QueryBuilder => _, _}
import shapeless._
import shapeless.ops.hlist.{Collect, Diff, Intersection, IsHCons, Length, Mapper, Prepend, Reify, RemoveAll, RightFolder, Take, ToList, ZipConst}
import shapeless.ops.record._
import poly.Case1
import shapeless.labelled._

import scala.util.Random


/**
  * Created by jolz on 8/05/16.
  */

object CassandraMapper {
  type Effect[A] = ReaderT[Task, SessionConfig, A]
  type CassandraDDL = (String, Create)

  case class TableNameDetails(existingTables: Set[String], baseName: String, pkNames: Seq[String], skNames: Seq[String])

  case class CassandraMapperConfig(tableNamer: TableNameDetails => String)

  val defaultTableNamer =
    (td: TableNameDetails) => {
      val bn = td.baseName
      if (td.existingTables(bn)) {
        (2 to 1000).iterator.map(n => s"${bn}_$n").find(n => !td.existingTables(n)).getOrElse(sys.error(s"Couldn't generate a unique table name for ${bn}"))
      } else bn
    }
}

trait CassandraTables[T, CR <: HList, CVL <: HList, PKL, SKL, XSKL, PKV, SKV] extends DepFn2[ColumnMapper[T, CR, CVL], String] {
  type Out = PhysRelation.Aux[Effect, CassandraDDL, T, PKV, SKV]
}

case class CassandraProjection[A](materializer: ColumnMaterialzer[CassandraColumn] => Option[A])

case class CassandraWhere(clauses: List[Clause]) {
  def addToSelect(q: Select) = {
    clauses.foreach(q.where);
    q
  }

  def addToUpdate(q: Update) = {
    clauses.foreach(q.where);
    q
  }

  def addToDelete(q: Delete) = {
    clauses.foreach(q.where);
    q
  }
}

class CassandraMapper(tableNamer: TableNameDetails => String = defaultTableNamer) extends RelationMapper[Effect] {

  type ColumnAtom[A] = CassandraColumn[A]
  type MapperConfig = CassandraMapperConfig
  type DDLStatement = (String, Create)
  type KeyMapperT = CassandraKeyMapper
  type KeyMapperPoly = CassandraMapper2.type
  type QueriesPoly = ConvertQueries2.type

  val config = CassandraMapperConfig(tableNamer)
  val M = Applicative[Effect]
  val C = implicitly[Catchable[Effect]]

  val stdColumnMaker = new MappingCreator[CassandraColumn] {
    def wrapAtom[S, A](atom: CassandraColumn[A], to: (S) => A, from: (A) => S): CassandraColumn[S] = WrappedColumn[S, A](atom, to, from)
  }
}

trait CassandraKeyMapper

object CassandraTables {
  implicit def cassandraTable[T, CR <: HList, CVL <: HList,
  PKL <: HList, SKL <: HList, XSKL <: HList,
  PKV, SKV <: HList, XSKV,
  PKCL <: HList, SKCL <: HList, XSKCL <: HList,
  CL <: HList]
  (implicit
   allPKC: SelectAll.Aux[CR, PKL, PKCL],
   allSKC: SelectAll.Aux[CR, SKL, SKCL],
   allXSKC: SelectAll.Aux[CR, XSKL, XSKCL],
   pkValB: PhysicalValues.Aux[CassandraColumn, PKCL, PKV, List[PhysicalValue[CassandraColumn]]],
   skValB: PhysicalValues.Aux[CassandraColumn, SKCL, SKV, List[PhysicalValue[CassandraColumn]]],
   xskValB: PhysicalValues.Aux[CassandraColumn, XSKCL, XSKV, List[PhysicalValue[CassandraColumn]]],
   allCols: Values.Aux[CR, CL],
   colList: ToList[CL, ColumnMapping[CassandraColumn, T, _]],
   skNL: ColumnNames[SKCL],
   xskNL: ColumnNames[XSKCL],
   pkNL: ColumnNames[PKCL],
   helperB: ColumnListHelperBuilder[CassandraColumn, T, CR, CVL, PKV :: SKV :: XSKV :: HNil],
   extractFullKey: ValueExtractor.Aux[CR, CVL, PKL :: SKL :: XSKL :: HNil, PKV :: SKV :: XSKV :: HNil]
  ) = new CassandraTables[T, CR, CVL, PKL, SKL, XSKL, PKV, SKV] {
    def apply(mapper: ColumnMapper[T, CR, CVL], tableName: String): PhysRelation.Aux[Effect, CassandraDDL, T, PKV, SKV]
    = new PhysRelation[Effect, CassandraDDL, T] {
      type PartitionKey = PKV
      type SortKey = SKV
      type XFullKey = PKV :: SKV :: XSKV :: HNil

      type Where = CassandraWhere
      type Projection[A] = CassandraProjection[A]

      type ClauseF = (String, AnyRef) => Clause

      val keyHList = extractFullKey()
      val helper = helperB(mapper, keyHList)
      private val columns = mapper.columns
      val pkVals = pkValB(allPKC(columns))
      val skVals = skValB(allSKC(columns))
      val xskVals = xskValB(allXSKC(columns))
      val skNames = skNL(allSKC(columns))
      val xskNames = xskNL(allXSKC(columns))
      val pkNames = pkNL(allPKC(columns))
      val allColumns = colList(allCols(columns))

      def getFullKey(t: T) = keyHList(mapper.toColumns(t))

      def fullKeyValues(fkVals: XFullKey) = pkVals(fkVals.head) ++ skVals(fkVals.tail.head) ++ xskVals(fkVals.tail.tail.head)

      def binding[A](pv: PhysicalValue[CassandraColumn]) = pv.atom.binding(pv.v)

      def valsToClauses(cols: List[PhysicalValue[CassandraColumn]], f: (String, AnyRef) => Clause) = cols.map {
        pv => f(CassandraSession.escapeReserved(pv.name), binding(pv))
      }

      def valsToWhere(cols: List[PhysicalValue[CassandraColumn]]) = CassandraWhere(valsToClauses(cols, QueryBuilder.eq))

      def createWriteQueries: WriteQueries[Effect, T] = new WriteQueries[Effect, T] {

        def deleteWithKey(fk: XFullKey, s: SessionConfig) = {
          val deleteq = valsToWhere(fullKeyValues(fk)).addToDelete(QueryBuilder.delete().all().from(tableName))
          s.executeLater(deleteq)
        }

        def insertWithVals(vals: Seq[PhysicalValue[CassandraColumn]], s: SessionConfig) = {
          val insert = QueryBuilder.insertInto(tableName)
          vals.foreach {
            pv => insert.value(CassandraSession.escapeReserved(pv.name), binding(pv))
          }
          s.executeLater(insert)
        }

        def delete(t: T): Effect[Unit] = ReaderT { s =>
          deleteWithKey(getFullKey(t), s).map(_ => ())
        }

        def insert(t: T): Effect[Unit] = ReaderT { s =>
          insertWithVals(helper.toPhysicalValues(t), s).map(_ => ())
        }

        def assignment(pv: ValueDifference[CassandraColumn]) = pv.atom.assignment(pv.name, pv.existing, pv.newValue)

        def update(existing: T, newValue: T): Effect[Boolean] = ReaderT { s =>
          helper.changeChecker(existing, newValue).map {
            case Xor.Right((oldKey, newKey, vals)) => deleteWithKey(oldKey, s) *> insertWithVals(vals, s)
            case Xor.Left((fk, diff)) =>
              val updateQ = QueryBuilder.update(tableName)
              diff.foreach {
                vd => updateQ.`with`(assignment(vd))
              }
              valsToWhere(fullKeyValues(fk)).addToUpdate(updateQ)
              s.executeLater(updateQ)
          } map (_.map(_ => true)) getOrElse Task.now(false)
        }
      }

      def createDDL = {
        val create = SchemaBuilder.createTable(tableName)
        val dts = allColumns.map(cm => cm.name -> cm.atom.dataType).toMap
        def addCol(f: (String, DataType) => Create)(name: String) = f(CassandraSession.escapeReserved(name), dts(name))
        val rskNames = skNames ++ xskNames
        pkNames.map(addCol(create.addPartitionKey))
        rskNames.map(addCol(create.addClusteringColumn))
        (dts.keySet -- (pkNames ++ rskNames)).toList.map(addCol(create.addColumn))
        (tableName, create)
      }

      def whereFullKey(pk: FullKey): Where = valsToWhere(pkVals(pk.head) ++ skVals(pk.tail.head))

      def rangeToClause(inc: ClauseF, exc: ClauseF, rv: RangeValue[SortKey]) = rv match {
        case NoRange => List.empty[Clause]
        case Inclusive(a) => valsToClauses(skVals(a), inc)
        case Exclusive(a) => valsToClauses(skVals(a), exc)
      }

      def whereRange(pk: PKV, lower: RangeValue[SortKey], upper: RangeValue[SortKey]): Where = {
        val keyClauses = valsToClauses(pkVals(pk), QueryBuilder.eq)
        val lowerClauses = rangeToClause(QueryBuilder.gte, QueryBuilder.gt, lower)
        val higherClauses = rangeToClause(QueryBuilder.lte, QueryBuilder.lt, upper)
        CassandraWhere(keyClauses ++ lowerClauses ++ higherClauses)
      }

      def selectAll = new CassandraProjection[T](helper.materializer)

      def rowMaterializer(r: Row) = new ColumnMaterialzer[CassandraColumn] {
        def apply[A](name: String, atom: CassandraColumn[A]): Option[A] = atom.byName(r, name)
      }

      def createReadQueries: ReadQueries = new ReadQueries {

        def createSelect[A](p: CassandraProjection[A], where: CassandraWhere) =
          where.addToSelect(QueryBuilder.select().all().from(tableName))

        def resultSetStream(stmt: Statement): Stream[Effect, ResultSet] = Stream.eval[Effect, ResultSet] {
          ReaderT { s => s.executeLater(stmt) }
        }

        def selectMany[A](projection: CassandraProjection[A], where: CassandraWhere, ascO: Option[Boolean]): Stream[Effect, A] = {
          val _select = createSelect(projection, where)
          val of = ascO.filter(_ => skNames.nonEmpty).map(asc => _select.orderBy(
            skNames.map(if (asc) QueryBuilder.asc else QueryBuilder.desc): _*)
          )
          val select = of.getOrElse(_select)
          resultSetStream(select).flatMap(rs => CassandraSession.rowsStream(rs).translate(effect2Task))
            .map(r => projection.materializer(rowMaterializer(r))).collect { case Some(e) => e }
        }

        def selectOne[A](projection: CassandraProjection[A], where: CassandraWhere): Effect[Option[A]] = ReaderT { s =>
          val select = createSelect(projection, where)
          s.executeLater(select).map {
            rs => Option(rs.one()).flatMap {
              r => projection.materializer(rowMaterializer(r))
            }
          }
        }
      }
    }
  }
}

object CassandraKeyMapper {

  type Aux[T, CR <: HList, KL <: HList, CVL <: HList, Q, PKK, PKV, SKK, SKV] = CassandraKeyMapper with KeyMapper[T, CR, KL, CVL, Q] {
    type Out = PhysRelation.Aux[Effect, CassandraDDL, T, PKV, SKV]
    type PartitionKey = PKV
    type SortKey = SKV
    type PartitionKeyNames = PKK
    type SortKeyNames = SKK
  }

  def apply[T, CR <: HList, KL <: HList, CVL <: HList, Q, PKK, PKV, SKK, XSKK, SKV]
  (tableCreator: CassandraTables[T, CR, CVL, PKK, SKK, XSKK, PKV, SKV])
  : Aux[T, CR, KL, CVL, Q, PKK, PKV, SKK, SKV]
  = new CassandraKeyMapper with KeyMapper.Impl[T, CR, KL, CVL, Q, PKK, PKV, SKK, SKV, PhysRelation.Aux[Effect, CassandraDDL, T, PKV, SKV]] {
    def keysMapped(cm: ColumnMapper[T, CR, CVL])(name: String) = tableCreator(cm, name)
  }

  implicit def uniquePK[K, T, CR <: HList, KL <: HList, CVL <: HList, PKV, SKV]
  (implicit
   tableCreator: CassandraTables[T, CR, CVL, KL, HNil, HNil, PKV, SKV]
  ): Aux[T, CR, KL, CVL, QueryPK[K], KL, PKV, HNil, SKV]
  = CassandraKeyMapper(tableCreator)

  implicit def uniqueResultMapper[K, T, CR <: HList, KL <: HList, CVL <: HList, UCL <: HList, SKL <: HList, PKV, SKV]
  (implicit
   mustHaveOne: IsHCons[UCL],
   sortKeys: RemoveAll.Aux[KL, UCL, (UCL, SKL)],
   tableCreator: CassandraTables[T, CR, CVL, UCL, SKL, HNil, PKV, SKV]
  ): Aux[T, CR, KL, CVL, QueryPK[K], UCL, PKV, SKL, SKV]
  = CassandraKeyMapper(tableCreator)

  implicit def multipleResultMapper[K, T, CR <: HList, PKL <: HList, CVL <: HList, UCL <: HList,
  SKL <: HList, PKV, SKV, ICL <: HList, PKLeftOver <: HList,
  PKWithoutUCL <: HList, FullSKL <: HList, ISKL <: HList, PKWithoutSK <: HList]
  (implicit
   intersect: Intersection.Aux[PKL, UCL, ICL],
   pkWithOutSC: RemoveAll.Aux[PKL, ICL, (ICL, PKWithoutUCL)],
   intersectSort: Intersection.Aux[PKWithoutUCL, SKL, ISKL],
   pkWithOutSK: RemoveAll.Aux[PKWithoutUCL, ISKL, (ISKL, PKWithoutSK)],
   tableCreator: CassandraTables[T, CR, CVL, UCL, SKL, PKWithoutSK, PKV, SKV]
  ): Aux[T, CR, PKL, CVL, QueryMultiple[K, UCL, SKL], UCL, PKV, SKL, SKV]
  = CassandraKeyMapper(tableCreator)

}

case class CassandraTable[K, PKL, SKL](name: String = "")

object CassandraMapper2 extends Poly1 {

  implicit def byPK[K, T, CR <: HList, KL <: HList, CVL <: HList]
  = at[(QueryPK[K], RelationDef[T, CR, KL, CVL])] {
    case (q, relation) => CassandraTable[K, KL, HNil]()
  }

  implicit def queryMulti[K, Cols <: HList, SortCols <: HList,
  T, CR <: HList, KL <: HList, CVL <: HList,
  LeftOverKL <: HList, LeftOverKL2 <: HList]
  (implicit
   diff1: Diff.Aux[KL, Cols, LeftOverKL],
   diff2: Diff.Aux[LeftOverKL, SortCols, LeftOverKL2],
   prepend: Prepend[SortCols, LeftOverKL2])
  = at[(QueryMultiple[K, Cols, SortCols], RelationDef[T, CR, KL, CVL])] {
    case (q, relation) => CassandraTable[K, Cols, prepend.Out]()
  }

  implicit def writes[K, RD] = at[(RelationWriter[K], RD)](_ => ())

}

object ConvertQueries2 extends Poly3 {

  def exactMatch[T](s: Seq[ColumnMapping[CassandraColumn, T, _]]) = s.map(c => CassandraEQ(c.name))

  def valsToBinding(s: Seq[PhysicalValue[CassandraColumn]]) = s.map(pv => pv.atom.binding(pv.v))

  def rowMaterializer(r: Row) = new ColumnMaterialzer[CassandraColumn] {
    def apply[A](name: String, atom: CassandraColumn[A]): Option[A] = atom.byName(r, name)
  }

  case class BuilderState[Created, Available, Builders, Writers](tablesCreated: Created, tablesAvailable: Available,
                                                                 builders: Builders, writers: Writers,
                                                                 ddl: Eval[Vector[CassandraDDL]], tableNamer: TableNameDetails => String,
                                                                 tableNames: Set[String] = Set.empty
                                                                )

  case class QueryCreate[K, PKL, SKL, Out](matched: CassandraTable[K, PKL, SKL], build: String => Out)

  trait mapQueryLP2 extends Poly1 {
    implicit def fallback[Q, RD]
    = at[(Q, RD, HNil)] {
      case (q, rd, table) => (s: String) => "Nothing found"
    }
  }

  trait mapQueryLP extends mapQueryLP2 {
    implicit def nextEntry[Q, RD, H, T <: HList](implicit tailEntry: Case1[mapQuery.type, (Q, RD, T)]) = at[(Q, RD, H :: T)] {
      case (q, rd, l) => tailEntry(q, rd, l.tail)
    }
  }

  object mapQuery extends mapQueryLP {

    implicit def pkQuery[K, T, CR <: HList, PKL <: HList, CVL <: HList,
    KL <: HList, SKL <: HList, Tail <: HList, AllKL <: HList, PKV]
    (implicit
     matchKL: Diff.Aux[KL, PKL, HNil],
     allColsKeys: Keys.Aux[CR, AllKL],
     pkColsLookup: ColumnsAsSeq.Aux[CR, PKL, T, CassandraColumn, PKV],
     allColsLookup: ColumnsAsSeq.Aux[CR, AllKL, T, CassandraColumn, CVL],
     materializer: MaterializeFromColumns.Aux[CassandraColumn, CR, CVL]
    )
    = at[(QueryPK[K], RelationDef[T, CR, PKL, CVL], CassandraTable[K, KL, SKL] :: Tail)] {
      case (q, rd, table) => QueryCreate[K, KL, SKL, UniqueQuery[Effect, T, PKV]](table.head, { table =>
        val columns = rd.columns
        val (allCols, allValsP) = allColsLookup(columns)
        val (pkCols, pkPhysV) = pkColsLookup(columns)
        val materialize = materializer(columns)
        val select = CassandraSelect(table, allCols.map(_.name), exactMatch(pkCols), Seq.empty, false)
        def doQuery(v: PKV): Effect[Option[T]] = ReaderT { s =>
          s.prepareAndBind(select, valsToBinding(pkPhysV(v))).map { rs =>
            Option(rs.one()).flatMap(r => materialize(rowMaterializer(r))).map(rd.mapper.fromColumns)
          }
        }
        UniqueQuery(doQuery)
      })
    }

    implicit def rangeQuery[K, Cols <: HList, SortCols <: HList, Q, T, RD, CR <: HList, CVL <: HList,
    PKL <: HList, KL <: HList, SKL <: HList, Tail <: HList, CRK <: HList,
    ColsVals <: HList, SortVals <: HList, SortLen <: Nat]
    (implicit
     evSame: Cols =:= KL,
     lenSC: Length.Aux[SortCols, SortLen],
     firstSK: Take.Aux[SKL, SortLen, SortCols],
     allKeys: Keys.Aux[CR, CRK],
     allColsLookup: ColumnsAsSeq.Aux[CR, CRK, T, CassandraColumn, CVL],
     pkColsLookup: ColumnsAsSeq.Aux[CR, Cols, T, CassandraColumn, ColsVals],
     skColsLookup: ColumnsAsSeq.Aux[CR, SortCols, T, CassandraColumn, SortVals],
     materializer: MaterializeFromColumns.Aux[CassandraColumn, CR, CVL]
    )
    = at[(QueryMultiple[K, Cols, SortCols], RelationDef[T, CR, PKL, CVL], CassandraTable[K, KL, SKL] :: Tail)] {
      case (q, rd, table) => QueryCreate[K, KL, SKL, RangeQuery[Effect, T, ColsVals, SortVals]](table.head, { tn =>
        val columns = rd.columns
        val (pkCols, pkPhysV) = pkColsLookup(columns)
        val (skCols, skPhysV) = skColsLookup(columns)
        val oAsc = skCols.map(cm => (cm.name, true))
        val oDesc = oAsc.map(o => (o._1, false))
        val (allCols, _) = allColsLookup(columns)
        val materialize = materializer(columns) andThen (_.map(rd.mapper.fromColumns))
        val baseSelect = CassandraSelect(tn, allCols.map(_.name), exactMatch(pkCols), Seq.empty, false)

        def doQuery(c: ColsVals, lr: RangeValue[SortVals], ur: RangeValue[SortVals], asc: Option[Boolean]): Stream[Effect, T] = {
          Stream.eval[Effect, ResultSet] {
            ReaderT { s =>
              def processOp(op: Option[(SortVals, String => CassandraClause)]) = op.map { case (sv, f) =>
                val vals = skPhysV(sv)
                (vals.map(pv => f(pv.name)), vals)
              }.getOrElse(Seq.empty, Seq.empty)

              val (lw, lv) = processOp(lr.fold(CassandraGTE, CassandraGT))
              val (uw, uv) = processOp(ur.fold(CassandraLTE, CassandraLT))
              val ordering = asc.map(a => if (a) oAsc else oDesc).getOrElse(Seq.empty)
              val select = baseSelect.copy(where = baseSelect.where ++ lw ++ uw, ordering=ordering)
              s.prepareAndBind(select, valsToBinding(pkPhysV(c) ++ lv ++ uv))
            }
          }.flatMap(rs => CassandraSession.rowsStream(rs).translate(effect2Task))
            .map(r => materialize(rowMaterializer(r))).collect { case Some(e) => e }
        }
        RangeQuery(None, doQuery)
      })
    }
  }


  trait foldQueriesLP extends Poly2 {
    implicit def buildNewTable[Q, T, CR <: HList, KL <: HList, CVL <: HList, Used <: HList,
    Available, Builders <: HList, Writers <: HList, K, PKL, SKL, Out, AllK <: HList, PKV, SKV]
    (implicit
     mq: Case1.Aux[mapQuery.type, (Q, RelationDef[T, CR, KL, CVL], Available), QueryCreate[K, PKL, SKL, Out]],
     update: UpdateOrAdd[Writers, K, WriteQueries[Effect, T]],
     allColKeys: Keys.Aux[CR, AllK],
     keysReified: Reify[AllK],
     allCols: ColumnsAsSeq[CR, AllK, T, CassandraColumn],
     pkColLookup: ColumnsAsSeq.Aux[CR, PKL, T, CassandraColumn, PKV],
     skColLookup: ColumnsAsSeq.Aux[CR, SKL, T, CassandraColumn, SKV],
     keyVals: ValueExtractor.Aux[CR, CVL, PKL :: SKL :: HNil, PKV :: SKV :: HNil],
     helperB: ColumnListHelperBuilder[CassandraColumn, T, CR, CVL, PKV :: SKV :: HNil]
    )
    = at[(Q, RelationDef[T, CR, KL, CVL]), BuilderState[Used, Available, Builders, Writers]] {
      case ((q, rd), bs) =>
        val extractKey = keyVals()
        val helper = helperB(rd.mapper, extractKey)
        val columnsRecord = rd.mapper.columns
        val (columns, _) = allCols(columnsRecord)
        val (pkCols, pkPhys) = pkColLookup(columnsRecord)
        val (skCols, skPhys) = skColLookup(columnsRecord)
        val skNames = skCols.map(_.name)
        val pkNames = pkCols.map(_.name)
        val tableName = bs.tableNamer(TableNameDetails(bs.tableNames, rd.baseName, pkNames, skNames))

        def createDDL = {
          val kr = keysReified()
          val create = SchemaBuilder.createTable(tableName)
          val dts = columns.map(cm => cm.name -> cm.atom.dataType).toMap
          def addCol(f: (String, DataType) => Create)(name: String) = f(CassandraSession.escapeReserved(name), dts(name))
          pkNames.map(addCol(create.addPartitionKey))
          skNames.map(addCol(create.addClusteringColumn))
          (dts.keySet -- (pkNames ++ skNames)).toList.map(addCol(create.addColumn))
          (tableName, create)
        }

        val writer = new WriteQueries[Effect, T] {
          val keyEq = exactMatch(pkCols ++ skCols)
          val insertQ = CassandraInsert(tableName, columns.map(_.name))
          val updateQ = CassandraUpdate(tableName, Seq.empty, keyEq)
          val deleteQ = CassandraDelete(tableName, keyEq)

          def keyBindings(key: PKV :: SKV :: HNil) = valsToBinding(pkPhys(key.head) ++ skPhys(key.tail.head))

          def deleteWithKey(key: PKV :: SKV :: HNil, s: SessionConfig): Task[Unit] =
            s.prepareAndBind(deleteQ, keyBindings(key)).map(_ => ())

          def insertWithVals(vals: Seq[PhysicalValue[CassandraColumn]], s: SessionConfig) =
            s.prepareAndBind(insertQ, valsToBinding(vals)).map(_ => ())

          def delete(t: T): Effect[Unit] = ReaderT { s => deleteWithKey(helper.extractKey(t), s) }

          def insert(t: T): Effect[Unit] = ReaderT { cs =>
            insertWithVals(helper.toPhysicalValues(t), cs)
          }

          def update(existing: T, newValue: T): Effect[Boolean] = ReaderT { s =>
            helper.changeChecker(existing, newValue).map {
              case Xor.Right((oldKey, newKey, vals)) => deleteWithKey(oldKey, s) *> insertWithVals(vals, s)
              case Xor.Left((fk, diff)) =>
                val assignments = diff.map(vd => vd.atom.assigner(vd.name, vd.existing, vd.newValue))
                s.prepareAndBind(updateQ.copy(assignments = assignments.map(_._1)), assignments.map(_._2))
            } map (_.map(_ => true)) getOrElse Task.now(false)

          }
        }
        val cq = mq(q, rd, bs.tablesAvailable)
        bs.copy[CassandraTable[K, PKL, SKL] :: Used, Available, Out :: Builders, update.Out](
          tablesCreated = cq.matched.copy[K, PKL, SKL](name = tableName) :: bs.tablesCreated,
          builders = cq.build(tableName) :: bs.builders,
          writers = update(bs.writers, ex => ex.map(x => WriteQueries.combine(x, writer)).getOrElse(writer)),
          ddl = bs.ddl.map(_ :+ createDDL),
          tableNames = bs.tableNames + tableName
        )
    }
  }

  object foldQueries extends foldQueriesLP {
    implicit def writeQuery[K, T, CR <: HList, PKL <: HList, CVL <: HList, Used, Available, Builders <: HList, Writers <: HList]
    = at[(RelationWriter[K], RelationDef[T, CR, PKL, CVL]), BuilderState[Used, Available, Builders, Writers]] {
      case ((q, rd), bs) => bs.copy[Used, Available, RelationWriter[K] :: Builders, Writers](builders = q :: bs.builders)
    }

    implicit def existingTable[Q, RD, Used, Available, Builders <: HList, Writers <: HList, K, KL, SKL, Out]
    (implicit mq: Case1.Aux[mapQuery.type, (Q, RD, Used), QueryCreate[K, KL, SKL, Out]])
    = at[(Q, RD), BuilderState[Used, Available, Builders, Writers]] {
      case ((q, rd), bs) =>
        val cq = mq(q, rd, bs.tablesCreated)
        bs.copy[Used, Available, Out :: Builders, Writers](builders = cq.build(cq.matched.name) :: bs.builders)
    }
  }

  trait finishWritesLP extends Poly2 {
    implicit def any[A, B] = at[A, B]((a, b) => b)
  }

  object finishWrites extends finishWritesLP {
    implicit def getWriter[K, W <: HList](implicit s: Selector[W, K]) = at[W, RelationWriter[K]] { case (w, _) => s(w) }
  }

  object tablesWithSK extends Poly1 {
    implicit def withSK[CT, SK <: HList](implicit ev: CT <:< CassandraTable[_, _, SK], ishCons: IsHCons[SK]) = at[CT](identity)
  }

  object tablesNoSK extends Poly1 {
    implicit def noSK[CT, SK <: HList](implicit ev: CT <:< CassandraTable[_, _, HNil]) = at[CT](identity)
  }

  implicit def convertAll[Q <: HList, Tables <: HList, WithSK <: HList, NoSK <: HList,
  SortedTables <: HList, QueriesAndTables <: HList, Created, OutQueries <: HList, Writers]
  (implicit
   collect: Collect.Aux[Tables, tablesWithSK.type, WithSK],
   collect2: Collect.Aux[Tables, tablesNoSK.type, NoSK],
   prepend: Prepend.Aux[WithSK, NoSK, SortedTables],
   folder: RightFolder.Aux[Q, BuilderState[HNil, SortedTables, HNil, HNil], foldQueries.type, BuilderState[Created, SortedTables, OutQueries, Writers]],
   finishUp: MapWith[Writers, OutQueries, finishWrites.type]
  ) = at[Q, Tables, CassandraMapperConfig] { (q, tables, config) =>
    val folded = folder(q, BuilderState(HNil, prepend(collect(tables), collect2(tables)), HNil, HNil, Eval.now(Vector.empty), config.tableNamer))
    val outQueries = finishUp(folded.writers, folded.builders)
    BuiltQueries[finishUp.Out, CassandraDDL](outQueries, folded.ddl.map(a => a))
  }
}