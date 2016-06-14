package io.doolse.simpledba.cassandra

import cats.Applicative
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
import io.doolse.simpledba.{QueryBuilder => _, _}
import shapeless._
import shapeless.ops.hlist.{Intersection, IsHCons, RemoveAll, ToList}
import shapeless.ops.record.{SelectAll, Values}


/**
  * Created by jolz on 8/05/16.
  */

object CassandraMapper {
  type Effect[A] = ReaderT[Task, SessionConfig, A]
  type CassandraDDL = (String, Create)
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

class CassandraMapper extends RelationMapper[Effect] {

  type ColumnAtom[A] = CassandraColumn[A]
  type DDLStatement = (String, Create)
  type KeyMapperT = CassandraKeyMapper

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

        def insertWithVals(vals: List[PhysicalValue[CassandraColumn]], s: SessionConfig) = {
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
  ): Aux[T, CR, KL, CVL, QueryUnique[K, HNil], KL, PKV, HNil, SKV]
  = CassandraKeyMapper(tableCreator)

  implicit def uniqueResultMapper[K, T, CR <: HList, KL <: HList, CVL <: HList, UCL <: HList, SKL <: HList, PKV, SKV]
  (implicit
   mustHaveOne: IsHCons[UCL],
   sortKeys: RemoveAll.Aux[KL, UCL, (UCL, SKL)],
   tableCreator: CassandraTables[T, CR, CVL, UCL, SKL, HNil, PKV, SKV]
  ): Aux[T, CR, KL, CVL, QueryUnique[K, UCL], UCL, PKV, SKL, SKV]
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
