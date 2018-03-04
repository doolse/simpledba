package io.doolse.simpledba2

import java.sql._

import cats.data.StateT
import cats.effect.IO
import fs2._
import io.doolse.simpledba2.Relation.DBIO
import shapeless._
import shapeless.labelled.FieldType
import cats.effect.implicits._
import shapeless.ops.hlist.{ToList, ZipWithKeys}
import shapeless.ops.record.{Keys, SelectAll}

import scala.annotation.tailrec
import scala.collection.Set

trait JDBCColumn[A] {
  def sqlType: SQLType

  def nullable: Boolean

  def getByIndex: (Int, ResultSet) => Option[A]

  def bind: (Int, Any, Connection, PreparedStatement) => Unit
}

case class JDBCColumns[Repr <: HList](columns: Seq[NamedColumn])
{
  def bind(i: Int, con: Connection, ps: PreparedStatement, record: Repr): IO[Unit] = IO {
    @tailrec
    def loop(offs: Int, rec: HList): Unit = {
      rec match {
        case h :: tail =>
          val col = columns(offs)
          col.column.bind(offs+i, h, con, ps)
          loop(offs+1, tail)
        case HNil => ()
      }
    }
    loop(0, record)
  }

  def get(i: Int, rs: ResultSet) : IO[Repr] = IO {
    @tailrec
    def loop(offs: Int, l: HList) : HList = {
      if (offs < 0) l else {
        val col = columns(offs)
        col.column.getByIndex(offs + i, rs) match {
          case None => throw new Error(s"Column ${col.name} is null")
          case Some(v) => loop(offs - 1, v :: l)
        }
      }
    }
    loop(columns.length-1, HNil).asInstanceOf[Repr]
  }

  def subset[Keys](implicit ss: KeySubset[Repr, Keys]): JDBCColumns[ss.Out] = {
    val subCols = ss.apply().toSet
    JDBCColumns(columns.filter(c => subCols(c.name)))
  }
}

trait KeySubset[Repr, Keys] {
  type Out <: HList
  def apply() : List[String]
}

object KeySubset {
  type Aux[Repr, Keys, Out0] = KeySubset[Repr, Keys] {
    type Out = Out0
  }
  implicit def isSubset[Repr <: HList, K <: HList, KOut <: HList, Out0 <: HList, RecOut <: HList]
  (implicit sa: SelectAll.Aux[Repr, K, Out0], withKeys: ZipWithKeys.Aux[K, Out0, RecOut],
   keys : Keys.Aux[RecOut, KOut],
   toList: ToList[KOut, Symbol])
  : KeySubset.Aux[Repr, K, Out0] =
    new KeySubset[Repr, K] {
      type Out = Out0
      def apply() = toList(keys()).map(_.name)
    }
}

case class NamedColumn(name: String, primaryKey: Boolean, column: JDBCColumn[_])

trait JDBCTable[T] {
  type Repr <: HList
  type KeyRepr <: HList

  def name: String

  def all: JDBCColumns[Repr]
  def keys: JDBCColumns[KeyRepr]

  def generic: LabelledGeneric.Aux[T, Repr]
  def sqlMapping: JDBCSQLConfig
}

object JDBCTable {
  type Aux[T, Repr0, KeyRepr0] = JDBCTable[T] {
    type Repr = Repr0
    type KeyRepr = KeyRepr0
  }

  def apply[T, Repr0 <: HList, KeyRepr0 <: HList](_name: String, _gen: LabelledGeneric.Aux[T, Repr0], mapping: JDBCSQLConfig,
                                 _all: JDBCColumns[Repr0], _keys: JDBCColumns[KeyRepr0])
  : JDBCTable.Aux[T, Repr0, KeyRepr0] = {
    new JDBCTable[T] {
      type Repr = Repr0
      type KeyRepr = KeyRepr0

      def name = _name

      def sqlMapping = mapping

      def generic = _gen

      def all = _all

      def keys = _keys
    }
  }
}

trait JDBCRelationBuilder[C[_], Repr <: HList] {
  def columns: List[NamedColumn]
  def apply() : JDBCColumns[Repr] = JDBCColumns[Repr](columns.toIndexedSeq)
}


object JDBCRelationBuilder {
  implicit def hnilRelation[C[_]]: JDBCRelationBuilder[C, HNil] = new JDBCRelationBuilder[C, HNil] {
    override def columns: List[NamedColumn] = List.empty
  }

  implicit def hconsRelation[C[_] <: JDBCColumn[_], K <: Symbol, V, Tail <: HList]
  (implicit wk: Witness.Aux[K], headColumn: C[V],
   tailRelation: JDBCRelationBuilder[C, Tail]) = new JDBCRelationBuilder[C, FieldType[K, V] :: Tail] {
    override def columns: List[NamedColumn] =
      NamedColumn(wk.value.name, false, headColumn) :: tailRelation.columns
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

  def byPK[T, Repr, Key](table: JDBCTable.Aux[T, Repr, Key]): Stream[DBIO, Key] => Stream[DBIO, T] = {
    val select = JDBCSelect(table.name, table.all.columns.map(_.name),
      table.keys.columns.map(kc => EQ(kc.name)), Seq.empty, false)
    keys => {
      keys.flatMap { key =>
        rowsStream {
          StateT.inspectF { con =>
            for {
              ps <- IO {
                con.prepareStatement(JDBCPreparedQuery.asSQL(select, table.sqlMapping))
              }
              _ <- table.keys.bind(1, con, ps, key)
              rs <- IO(ps.executeQuery())
            } yield rs
          }
        }.evalMap {
          rs => table.all.get(1, rs).liftIO[DBIO]
        }.map(table.generic.from)
      }
    }
  }
}



