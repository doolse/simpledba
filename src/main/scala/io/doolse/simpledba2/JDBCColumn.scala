package io.doolse.simpledba2

import java.sql._

import cats.data.StateT
import cats.effect.IO
import fs2._
import io.doolse.simpledba2.Relation.DBIO
import shapeless._
import shapeless.labelled.FieldType
import cats.effect.implicits._
import shapeless.ops.hlist.{Length, Prepend, Split, Take, ToList, ZipWithKeys}
import shapeless.ops.record.{Keys, SelectAll}

import scala.annotation.tailrec
import scala.collection.Set

trait JDBCColumn[A] {
  def sqlType: SQLType

  def nullable: Boolean

  def getByIndex: (Int, ResultSet) => Option[A]

  def bind: (Int, Any, Connection, PreparedStatement) => Unit
}

case class JDBCColumns[T, Repr <: HList](columns: Seq[NamedColumn], to: T => Repr, from: Repr => T)
{
  def isomap[T2](to2: T => T2, from2: T2 => T) : JDBCColumns[T2, Repr] =
    copy[T2, Repr](to = from2.andThen(to), from = from.andThen(to2))

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

  def subset[Keys](implicit ss: KeySubset[Repr, Keys]): JDBCColumns[Repr, ss.Out] = {
    val subCols = ss.apply().toSet
    JDBCColumns(columns.filter(c => subCols(c.name)), _ => ???, _ => ???)
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

  def all: JDBCColumns[T, Repr]
  def keys: JDBCColumns[Repr, KeyRepr]

  def sqlMapping: JDBCSQLConfig
}

object JDBCTable {
  type Aux[T, Repr0, KeyRepr0] = JDBCTable[T] {
    type Repr = Repr0
    type KeyRepr = KeyRepr0
  }

  def apply[T, Repr0 <: HList, KeyRepr0 <: HList](_name: String, mapping: JDBCSQLConfig,
                                 _all: JDBCColumns[T, Repr0], _keys: JDBCColumns[Repr0, KeyRepr0])
  : JDBCTable.Aux[T, Repr0, KeyRepr0] = {
    new JDBCTable[T] {
      type Repr = Repr0
      type KeyRepr = KeyRepr0

      def name = _name

      def sqlMapping = mapping

      def all = _all

      def keys = _keys
    }
  }
}

trait JDBCRelationBuilder[C[_], T] {
  type Repr <: HList
  def apply() : JDBCColumns[T, Repr]
}


object JDBCRelationBuilder {
  type Aux [C[_], T, Repr0 <: HList] = JDBCRelationBuilder[C, T]
  {
    type Repr = Repr0
  }

  implicit def hnilRelation[C[_]]: JDBCRelationBuilder.Aux[C, HNil, HNil] =
    new JDBCRelationBuilder[C, HNil] {
      type Repr = HNil

      override def apply(): JDBCColumns[HNil, HNil] = JDBCColumns(Seq.empty, identity, identity)
    }

  implicit def embeddedField[C[_], K, V, Repr <: HList]
  (implicit embeddedCols: JDBCRelationBuilder.Aux[C, V, Repr]) : JDBCRelationBuilder.Aux[C, FieldType[K, V], Repr]
    = embeddedCols.asInstanceOf[JDBCRelationBuilder.Aux[C, FieldType[K, V], Repr]]

  implicit def singleColumn[C[_] <: JDBCColumn[_], K <: Symbol, V]
  (implicit wk: Witness.Aux[K], headColumn: C[V])
  : JDBCRelationBuilder.Aux[C, FieldType[K, V], FieldType[K, V] :: HNil]
  = new JDBCRelationBuilder[C, FieldType[K, V]] {
    type Repr = FieldType[K, V] :: HNil

    override def apply(): JDBCColumns[FieldType[K, V], ::[FieldType[K, V], HNil]] =
      JDBCColumns(Seq(NamedColumn(wk.value.name, false, headColumn)),
        _ :: HNil, _.head
      )
  }

  implicit def hconsRelation[C[_] <: JDBCColumn[_], H, HOut <: HList,
        HLen <: Nat, T <: HList, TOut <: HList, Out <: HList]
  (implicit
   headColumns: JDBCRelationBuilder.Aux[C, H, HOut],
   tailColumns: JDBCRelationBuilder.Aux[C, T, TOut],
   len: Length.Aux[HOut, HLen],
   append: Prepend.Aux[HOut, TOut, Out],
   split: Split.Aux[Out, HLen, HOut, TOut]
  ) : JDBCRelationBuilder.Aux[C, H :: T, Out] =
    new JDBCRelationBuilder[C, H :: T] {
      type Repr = Out

      override def apply(): JDBCColumns[::[H, T], Out] = {
        val hc = headColumns.apply()
        val tc = tailColumns.apply()
        JDBCColumns(hc.columns ++ tc.columns,
          t => append.apply(hc.to(t.head), tc.to(t.tail)),
          { o =>
            val (ho,to) = split(o)
            hc.from(ho) :: tc.from(to)
          })
      }
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
        }.map(table.all.from)
      }
    }
  }
}



