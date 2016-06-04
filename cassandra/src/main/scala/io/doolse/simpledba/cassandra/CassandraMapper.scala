package io.doolse.simpledba.cassandra

import cats.Monad
import cats.data.{ReaderT, Xor}
import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{Select, _}
import com.datastax.driver.core.schemabuilder.{Create, SchemaBuilder}
import fs2.util.Task
import fs2.interop.cats._
import io.doolse.simpledba.cassandra.CassandraMapper.Effect
import io.doolse.simpledba.{ColumnMapper, RelationMapper, WriteQueries}
import shapeless._
import shapeless.ops.hlist.{RemoveAll, ToList}
import shapeless.ops.record.{SelectAll, Values}
import cats.syntax.all._

import scala.collection.JavaConverters._

/**
  * Created by jolz on 8/05/16.
  */

object CassandraMapper {
  type Effect[A] = ReaderT[Task, SessionConfig, A]
}

class CassandraMapper extends RelationMapper[Effect, CassandraColumn] {

  trait CassandraTables[T, CR <: HList, CVL <: HList, PKL, SKL, PKV, SKV] extends DepFn2[ColumnMapper[T, CR, CVL], String] {
    type Out = PhysRelationImpl[T, PKV, SKV]
  }

  case class CassandraProjection[A](materializer: ColumnMaterialzer => Option[A])

  case class CassandraWhere(clauses: List[Clause]) {
    def addToSelect(q: Select) = { clauses.foreach(q.where); q }
    def addToUpdate(q: Update) = { clauses.foreach(q.where); q }
    def addToDelete(q: Delete) = { clauses.foreach(q.where); q }
  }


  type Where = CassandraWhere
  type Projection[A] = CassandraProjection[A]
  type DDLStatement = (String, Create)

  object CassandraTables {
    implicit def cassandraTable[T, CR <: HList, CVL <: HList,
    PKL <: HList, SKL <: HList, PKV, SKV <: HList, PKCL <: HList, SKCL <: HList,
    CL <: HList]
    (implicit
     allPKC: SelectAll.Aux[CR, PKL, PKCL],
     allSKC: SelectAll.Aux[CR, SKL, SKCL],
     pkValB: PhysicalValues.Aux[PKCL, PKV, List[PhysicalValue]],
     skValB: PhysicalValues.Aux[SKCL, SKV, List[PhysicalValue]],
     allCols: Values.Aux[CR, CL],
     colList: ToList[CL, ColumnMapping[T, _]],
     skNL: ColumnNames[SKCL],
     pkNL: ColumnNames[PKCL],
     helperB: ColumnListHelperBuilder[T, CR, CVL, PKV :: SKV :: HNil],
     extractFullKey: ValueExtractor.Aux[CR, CVL, PKL :: SKL :: HNil, PKV :: SKV :: HNil]
    ) = new CassandraTables[T, CR, CVL, PKL, SKL, PKV, SKV] {
      def apply(mapper: ColumnMapper[T, CR, CVL], tableName: String): PhysRelationImpl[T, PKV, SKV] = new PhysRelationImpl[T, PKV, SKV] {

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

        def binding[A]: (A, CassandraColumn[A]) => Any = (v, c) => c.binding(v)

        def valsToWhere(cols: List[PhysicalValue]) = CassandraWhere {
          cols.map {
            pv => QueryBuilder.eq(CassandraSession.escapeReserved(pv.name), pv.withCol(binding))
          }
        }

        def createWriteQueries: WriteQueries[Effect, T] = new WriteQueries[Effect, T] {

          def deleteWithKey(fk: FullKey, s: SessionConfig) = {
            val deleteq = valsToWhere(fullKeyValues(fk)).addToDelete(QueryBuilder.delete().all().from(tableName))
            s.executeLater(deleteq)
          }

          def insertWithVals(vals: List[PhysicalValue], s: SessionConfig) = {
            val insert = QueryBuilder.insertInto(tableName)
            vals.foreach {
              pv => insert.value(CassandraSession.escapeReserved(pv.name), pv.withCol(binding))
            }
            s.executeLater(insert)
          }

          def delete(t: T): Effect[Unit] = ReaderT { s =>
            deleteWithKey(getFullKey(t), s).map(_ => ())
          }

          def insert(t: T): Effect[Unit] = ReaderT { s =>
            insertWithVals(helper.toPhysicalValues(t), s).map(_ => ())
          }

          def update(existing: T, newValue: T): Effect[Boolean] = ReaderT { s =>
            helper.changeChecker(existing, newValue).map {
              case Xor.Right((oldKey, newKey, vals)) => deleteWithKey(oldKey, s) *> insertWithVals(vals, s)
              case Xor.Left((fk, diff)) =>
                val updateQ = QueryBuilder.update(tableName)
                diff.foreach {
                  vd => updateQ.`with`(vd.withCol((exV,newV,c) => c.assignment(vd.name, exV, newV)))
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

        def rowMaterializer(r: Row) = new ColumnMaterialzer {
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


  implicit def cassandraKeyMapper[T, CR <: HList, KL <: HList, CVL <: HList, PKL <: HList, SKLR, SKL <: HList, PKV, SKV]
  (implicit
   sortKeys: RemoveAll.Aux[KL, PKL, SKLR],
   ev: SKLR <:< (_, SKL),
   tableCreator: CassandraTables[T, CR, CVL, PKL, SKL, PKV, SKV]
  )
  = new KeyMapperImpl[T, CR, KL, CVL, PKL, PKL, PKV, SKL, SKV] {
    def keysMapped(cm: ColumnMapper[T, CR, CVL])(name: String): PhysRelationImpl[T, PKV, SKV] = tableCreator(cm, name)
  }

  def doWrapAtom[S, A](atom: CassandraColumn[A], to: (S) => A, from: (A) => S): CassandraColumn[S] = WrappedColumn[S, A](atom, to, from)
}
