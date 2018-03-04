package io.doolse.simpledba2

import java.sql._

import cats.data.StateT
import cats.effect.IO
import fs2._
import io.doolse.simpledba2.Relation.DBIO
import shapeless._
import shapeless.labelled.FieldType
import cats.effect.implicits._

import scala.collection.Set

trait JDBCColumn[A] {
  def sqlType: SQLType

  def nullable: Boolean

  def getByIndex: (Int, ResultSet) => Option[A]

  def bind: (Int, AnyRef, Connection, PreparedStatement) => Unit
}

case class NamedColumn(name: String, primaryKey: Boolean, column: JDBCColumn[_])

trait JDBCTable[T, Key] {
  type Repr

  def name: String

  def all: Seq[NamedColumn]

  def keyColumns: Seq[String]

  def keyJDBCValues: Key => Seq[AnyRef]

  def generic: LabelledGeneric.Aux[T, Repr]

  def sqlMapping: JDBCSQLConfig
}

object JDBCTable {
  type Aux[T, Key, Repr0] = JDBCTable[T, Key] {
    type Repr = Repr0
  }

  def apply[C[_], T, Key, Repr0](_name: String, _gen: LabelledGeneric.Aux[T, Repr0], mapping: JDBCSQLConfig,
                                 _all: JDBCRelationBuilder[C, Repr0], _key: Seq[String], _keyFunc: Key => Seq[AnyRef])
  : JDBCTable.Aux[T, Key, Repr0] = new JDBCTable[T, Key] {
    type Repr = Repr0

    def name = _name

    val all = _all.apply().toVector
    val keyColumns = _key.toVector

    def sqlMapping = mapping

    def generic = _gen

    def keyJDBCValues = _keyFunc
  }
}

trait JDBCRelationBuilder[C[_], Repr] {
  def apply(): List[NamedColumn]
}


object JDBCRelationBuilder {
  implicit def hnilRelation[C[_]]: JDBCRelationBuilder[C, HNil] = () => List.empty

  implicit def hconsRelation[C[_] <: JDBCColumn[_], K <: Symbol, V, Tail <: HList]
  (implicit wk: Witness.Aux[K], headColumn: C[V],
   tailRelation: JDBCRelationBuilder[C, Tail]) = new JDBCRelationBuilder[C, FieldType[K, V] :: Tail] {
    override def apply(): List[NamedColumn] =
      NamedColumn(wk.value.name, false, headColumn) :: tailRelation.apply()
  }
}

object Relation {

  type DBIO[A] = StateT[IO, Connection, A]

}


object Query {


  def rowsStream[A](open: DBIO[ResultSet]): Stream[DBIO, ResultSet] = {
    def nextLoop(rs: ResultSet): Stream[DBIO, ResultSet] =
      Stream.eval(IO(rs.next()).liftIO[DBIO]).flatMap {
        n => if (n) Stream(rs) ++ nextLoop(rs) else Stream.empty
      }

    Stream.bracket(open)(nextLoop, rs => IO(rs.close()).liftIO[DBIO])
  }

  def byPK[T, Key, Repr](table: JDBCTable.Aux[T, Key, Repr]): Stream[DBIO, Key] => Stream[DBIO, T] = {
    val keySet = table.keyColumns.toSet
    val keyCols = table.all.filter(c => keySet(c.name))
    keys => {
      keys.flatMap { key =>
        rowsStream {
          StateT.inspectF { con =>
            val select = JDBCSelect(table.name, table.all.map(_.name),
              table.keyColumns.map(kc => EQ(kc)), Seq.empty, false)
            for {
              ps <- IO {
                con.prepareStatement(JDBCPreparedQuery.asSQL(select, table.sqlMapping))
              }
              rs <- IO {
                val bindVals = table.keyJDBCValues(key).zipWithIndex.foreach {
                  case (v, i) => keyCols(i).column.bind(i + 1, v, con, ps)
                }
                ps.executeQuery()
              }
            } yield rs
          }
        }.map {
          rs =>
            def prepend(col: (NamedColumn, Int), l: HList) = {
              col._1.column.getByIndex(col._2+1, rs).fold(throw new Error(s"Column ${col._1.name} is null"))(r => r :: l)
            }

            table.generic.from(table.all.zipWithIndex.foldRight(HNil: HList)(prepend).asInstanceOf[Repr])
        }
      }
    }
  }
}



