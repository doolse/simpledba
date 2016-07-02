package io.doolse.simpledba

import cats.Eval
import io.doolse.simpledba.RelationMapper.{SimpleMapperConfig, TableNameDetails}
import shapeless._
import shapeless.ops.hlist.{RightFolder, ToList}
import shapeless.poly.Case2

/**
  * Created by jolz on 2/07/16.
  */

trait KeyBasedTable {
  def pkNames: Seq[String]
  def skNames: Seq[String]
}
case class QueryCreate[PT, Out](matcher: PT => Boolean, build: (String,PT) => Out)

trait QueryFolder[Effect[_], DDL, PhysicalTable[_] <: KeyBasedTable, MapQuery <: Poly2] extends Poly3 {

  case class TableKey[T](name: String)
  class CreatedMap[K, V]
  class AvailableMap[K, V]

  implicit def key2CreatedTableVector[T] = new CreatedMap[TableKey[T], Vector[(String, PhysicalTable[T])]]
  implicit def key2TableVector[T] = new AvailableMap[TableKey[T], Vector[PhysicalTable[T]]]

  case class BuilderState[Builders](relationBases: Iterable[String], created: HMap[CreatedMap], available: HMap[AvailableMap], builders: Builders,
                                    tableNamer: TableNameDetails => String, tableNames: Set[String] = Set.empty)


  def singleCreateList[T](bs: BuilderState[_], name: String) = bs.created.get(TableKey[T](name)).get
  def createTableDDL[T](s: String, table: PhysicalTable[T]): DDL
  def createDDL(bs: BuilderState[_]): Eval[Iterable[DDL]] = Eval.later {
    bs.relationBases.flatMap(n => singleCreateList(bs, n)).map {
      case (n,pt) => createTableDDL(n,pt)
    }
  }
  def createWriter[T](v: Vector[(String, PhysicalTable[T])]): WriteQueries[Effect, T]

  object foldQueries extends Poly2 {
    implicit def writeQuery[K, T, CR <: HList, PKL <: HList, CVL <: HList, Builders <: HList]
    = at[(RelationWriter[K], RelationDef[T, CR, PKL, CVL]), BuilderState[Builders]] {
      case ((q, rd), bs) => bs.copy[TableKey[T] :: Builders](builders = TableKey[T](rd.baseName) :: bs.builders)
    }
    implicit def buildNewTable[Q, T, CR <: HList, KL <: HList, CVL <: HList,
    Builders <: HList, Writers <: HList, K, Out]
    (implicit
     evQ: Q <:< RelationQuery[K],
     mq: Case2.Aux[MapQuery, Q, RelationDef[T, CR, KL, CVL], QueryCreate[PhysicalTable[T], Out]]
    )
    = at[(Q, RelationDef[T, CR, KL, CVL]), BuilderState[Builders]] {
      case ((q, rd), bs) =>
        val queryCreate = mq(q, rd)
        val tableKey = TableKey[T](rd.baseName)
        val alreadyCreated = bs.created.get(tableKey)
        alreadyCreated.flatMap {
          _.collectFirst {
            case (tableName, ct) if queryCreate.matcher(ct) =>
              bs.copy[Out :: Builders](builders = queryCreate.build(tableName, ct) :: bs.builders)
          }
        } orElse bs.available.get(tableKey).flatMap {
          _.collectFirst {
            case ct if queryCreate.matcher(ct) =>
              val tableName = bs.tableNamer(TableNameDetails(bs.tableNames, rd.baseName, ct.pkNames, ct.skNames))
              bs.copy[Out :: Builders](builders = queryCreate.build(tableName, ct) :: bs.builders,
                tableNames = bs.tableNames + tableName,
                created = bs.created + (tableKey, alreadyCreated.getOrElse(Vector.empty) :+ (tableName, ct)))
          }
        } getOrElse sys.error("Bug - should be able to find a matching table")
    }

  }

  trait finishWritesLP extends Poly2 {
    implicit def any[A, B] = at[A, B]((a, b) => b)
  }

  object finishWrites extends finishWritesLP {
    implicit def getWriter[Q, T] = at[BuilderState[Q], TableKey[T]] { (bs, tk) => bs.created.get(tk).map(createWriter).get }
  }

  implicit def convertAll[Q <: HList, Tables <: HList,
  Created, OutTables, OutQueries <: HList, Writers]
  (implicit
   toList: ToList[Tables, Any],
   folder: RightFolder.Aux[Q, BuilderState[HNil], foldQueries.type, BuilderState[OutQueries]],
   finishUp: MapWith[BuilderState[OutQueries], OutQueries, finishWrites.type]
  ) = at[Q, Tables, SimpleMapperConfig] {
    (q, tables, config) =>

      val tabList = tables.toList.collect {
        case (k: String, ct: KeyBasedTable) => (k, ct.asInstanceOf[PhysicalTable[_]])
      } sortBy (_._2.skNames.size)

      val bases = tabList.map(_._1).distinct
      def addToMap[T](m: HMap[AvailableMap], v: (String, PhysicalTable[T])) = {
        val k = TableKey[T](v._1)
        m + (k, m.get(k).getOrElse(Vector.empty) :+ v._2)
      }
      val tabMap = tabList.foldLeft(HMap.empty[AvailableMap])((m, v) => addToMap(m, v))
      val folded = folder(q, BuilderState(bases, HMap.empty, tabMap, HNil, config.tableNamer))
      val outQueries = finishUp(folded, folded.builders)
      BuiltQueries[finishUp.Out, DDL](outQueries, createDDL(folded))
  }

}
