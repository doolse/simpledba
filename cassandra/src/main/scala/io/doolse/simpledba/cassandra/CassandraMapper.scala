package io.doolse.simpledba.cassandra

import cats.Monad
import cats.data.{ReaderT, Xor}
import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{Select, _}
import com.datastax.driver.core.schemabuilder.{Create, SchemaBuilder}
import fs2.util.Task
import fs2.interop.cats._
import io.doolse.simpledba.cassandra.CassandraMapper._
import io.doolse.simpledba.{QueryBuilder => _, _}
import shapeless._
import shapeless.ops.hlist.{Intersection, IsHCons, RemoveAll, ToList}
import shapeless.ops.record.{SelectAll, Values}
import cats.syntax.all._
import io.doolse.simpledba.PhysRelation.Aux

import scala.collection.JavaConverters._

/**
  * Created by jolz on 8/05/16.
  */

object CassandraMapper {
  type Effect[A] = ReaderT[Task, SessionConfig, A]
  type CassandraDDL = (String, Create)
}

trait CassandraTables[T, CR <: HList, CVL <: HList, PKL, SKL, PKV, SKV] extends DepFn2[ColumnMapper[T, CR, CVL], String] {
  type Out = PhysRelation.Aux[Effect, CassandraDDL, T, PKV, SKV]
}

case class CassandraProjection[A](materializer: ColumnMaterialzer[CassandraColumn] => Option[A])

case class CassandraWhere(clauses: List[Clause]) {
  def addToSelect(q: Select) = { clauses.foreach(q.where); q }
  def addToUpdate(q: Update) = { clauses.foreach(q.where); q }
  def addToDelete(q: Delete) = { clauses.foreach(q.where); q }
}

class CassandraMapper extends RelationMapper[Effect] {

  type ColumnAtom[A] = CassandraColumn[A]
  type DDLStatement = (String, Create)
  type KeyMapperT = CassandraKeyMapper

  def doWrapAtom[S, A](atom: CassandraColumn[A], to: (S) => A, from: (A) => S): CassandraColumn[S] = WrappedColumn[S, A](atom, to, from)
}

trait CassandraKeyMapper

object CassandraTables {
  implicit def cassandraTable[T, CR <: HList, CVL <: HList,
  PKL <: HList, SKL <: HList, PKV, SKV <: HList, PKCL <: HList, SKCL <: HList,
  CL <: HList]
  (implicit
   allPKC: SelectAll.Aux[CR, PKL, PKCL],
   allSKC: SelectAll.Aux[CR, SKL, SKCL],
   pkValB: PhysicalValues.Aux[CassandraColumn, PKCL, PKV, List[PhysicalValue[CassandraColumn]]],
   skValB: PhysicalValues.Aux[CassandraColumn, SKCL, SKV, List[PhysicalValue[CassandraColumn]]],
   allCols: Values.Aux[CR, CL],
   colList: ToList[CL, ColumnMapping[CassandraColumn, T, _]],
   skNL: ColumnNames[SKCL],
   pkNL: ColumnNames[PKCL],
   helperB: ColumnListHelperBuilder[CassandraColumn, T, CR, CVL, PKV :: SKV :: HNil],
   extractFullKey: ValueExtractor.Aux[CR, CVL, PKL :: SKL :: HNil, PKV :: SKV :: HNil]
  ) = new CassandraTables[T, CR, CVL, PKL, SKL, PKV, SKV] {
    def apply(mapper: ColumnMapper[T, CR, CVL], tableName: String):PhysRelation.Aux[Effect, CassandraDDL, T, PKV, SKV]
    = new PhysRelation[Effect, CassandraDDL, T] {
      type PartitionKey = PKV
      type SortKey = SKV

      type Where = CassandraWhere
      type Projection[A] = CassandraProjection[A]

      val keyHList = extractFullKey()
      val helper = helperB(mapper, keyHList)
      private val columns = mapper.columns
      val pkVals = pkValB(allPKC(columns))
      val skVals = skValB(allSKC(columns))
      val skNames = skNL(allSKC(columns))
      val pkNames = pkNL(allPKC(columns))
      val allColumns = colList(allCols(columns))
      def getFullKey(t: T) = keyHList(mapper.toColumns(t))
      def fullKeyValues(fkVals: FullKey) = pkVals(fkVals.head) ++ skVals(fkVals.tail.head)

      def binding[A](pv:PhysicalValue[CassandraColumn]) = pv.atom.binding(pv.v)

      def valsToWhere(cols: List[PhysicalValue[CassandraColumn]]) = CassandraWhere {
        cols.map {
          pv => QueryBuilder.eq(CassandraSession.escapeReserved(pv.name), binding(pv))
        }
      }

      def createWriteQueries: WriteQueries[Effect, T] = new WriteQueries[Effect, T] {

        def deleteWithKey(fk: FullKey, s: SessionConfig) = {
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
        val pkNameSet = pkNames.toSet
        val skNS = skNames.toSet
        allColumns.foreach { cm =>
          val name = CassandraSession.escapeReserved(cm.name)
          val dt = cm.atom.dataType
          if (pkNameSet(name)) create.addPartitionKey(name, dt)
          else if (skNS(name)) create.addClusteringColumn(name, dt)
          else create.addColumn(name, dt)
        }
        (tableName, create)
      }

      def whereFullKey(pk: FullKey): Where = valsToWhere(pkVals(pk.head) ++ skVals(pk.tail.head))

      def whereRange(pk: PKV, lower: SKV, upper: SKV): Where = ???

      def wherePK(pk: PKV): Where = valsToWhere(pkVals(pk))

      def selectAll = new CassandraProjection[T](helper.materializer)

      def rowMaterializer(r: Row) = new ColumnMaterialzer[CassandraColumn] {
        def apply[A](name: String, atom: CassandraColumn[A]): Option[A] = atom.byName(r, name)
      }

      def createReadQueries: ReadQueries = new ReadQueries {

        def createSelect[A](p: CassandraProjection[A], where: CassandraWhere) =
          where.addToSelect(QueryBuilder.select().all().from(tableName))

        def selectMany[A](projection: CassandraProjection[A], where: CassandraWhere, ascO: Option[Boolean]): Effect[List[A]] = ReaderT { s =>
          val _select = createSelect(projection, where)
          val of = ascO.map(asc => _select.orderBy(skNames.map(if (asc) QueryBuilder.asc else QueryBuilder.desc): _*))
          val select = of.getOrElse(_select)
          s.executeLater(select).map {
            rs => rs.iterator().asScala.map {
              r => projection.materializer(rowMaterializer(r))
            }.flatten.toList
          }
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

  def apply[T, CR <: HList, KL <: HList, CVL <: HList, Q, PKK, PKV, SKK, SKV]
  (tableCreator: CassandraTables[T, CR, CVL, PKK, SKK, PKV, SKV])
  : Aux[T, CR, KL, CVL, Q, PKK, PKV, SKK, SKV]
  = new CassandraKeyMapper with KeyMapper.Impl[T, CR, KL, CVL, Q, PKK, PKV, SKK, SKV, PhysRelation.Aux[Effect, CassandraDDL, T, PKV, SKV]] {
    def keysMapped(cm: ColumnMapper[T, CR, CVL])(name: String) = tableCreator(cm, name)
  }

  implicit def uniquePK[K, T, CR <: HList, KL <: HList, CVL <: HList, PKV, SKV]
  (implicit
   tableCreator: CassandraTables[T, CR, CVL, KL, HNil, PKV, SKV]
  ) : Aux[T, CR, KL, CVL, QueryUnique[K, HNil], KL, PKV, HNil, SKV]
  = CassandraKeyMapper(tableCreator)

  implicit def uniqueResultMapper[K, T, CR <: HList, KL <: HList, CVL <: HList, UCL <: HList, SKLR, SKL <: HList, PKV, SKV]
  (implicit
   mustHaveOne: IsHCons[UCL],
   sortKeys: RemoveAll.Aux[KL, UCL, SKLR],
   ev: SKLR <:< (_, SKL),
   tableCreator: CassandraTables[T, CR, CVL, UCL, SKL, PKV, SKV]
  ) : Aux[T, CR, KL, CVL, QueryUnique[K, UCL], UCL, PKV, SKL, SKV]
  = CassandraKeyMapper(tableCreator)

  implicit def multipleResultMapper[K, T, CR <: HList, KL <: HList, CVL <: HList, UCL <: HList, SKLR,
  SKL <: HList, PKV, SKV, ICL <: HList]
  (implicit
   intersect: Intersection.Aux[KL, UCL, ICL],
   sortKeys: RemoveAll.Aux[KL, ICL, SKLR],
   ev: SKLR <:< (_, SKL),
   tableCreator: CassandraTables[T, CR, CVL, UCL, SKL, PKV, SKV]
  ) : Aux[T, CR, KL, CVL, QueryMultiple[K, UCL, HNil], UCL, PKV, SKL, SKV]
  = CassandraKeyMapper(tableCreator)
}
