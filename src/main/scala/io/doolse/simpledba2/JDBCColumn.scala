package io.doolse.simpledba2

import java.sql._

import cats.data.StateT
import cats.effect.IO
import cats.effect.implicits._
import fs2._
import io.doolse.simpledba2.Relation.DBIO
import shapeless._

import scala.annotation.tailrec

trait JDBCColumn {
  def sqlType: SQLType

  def nullable: Boolean

  def getByIndex: (Int, ResultSet) => Option[Any]

  def bind: (Int, Any, Connection, PreparedStatement) => Unit
}

trait JDBCTable[T] {
  type Repr <: HList
  type KeyRepr <: HList
  type JC[A] <: JDBCColumn

  def name: String

  def all: Columns[JC, T, Repr]
  def keys: Columns[JC, KeyRepr, KeyRepr]

  def sqlMapping: JDBCSQLConfig
}

object JDBCTable {
  type Aux[T, Repr0, KeyRepr0] = JDBCTable[T] {
    type Repr = Repr0
    type KeyRepr = KeyRepr0
  }

  def apply[C[_] <: JDBCColumn, T, Repr0 <: HList, KeyRepr0 <: HList](_name: String, mapping: JDBCSQLConfig,
                                 _all: Columns[C, T, Repr0], _keys: Columns[C, KeyRepr0, KeyRepr0])
  : JDBCTable.Aux[T, Repr0, KeyRepr0] = {
    new JDBCTable[T] {
      type Repr = Repr0
      type KeyRepr = KeyRepr0
      type JC[A] = C[A]

      def name = _name

      def sqlMapping = mapping

      def all = _all

      def keys = _keys
    }
  }
}

object Relation {

  type DBIO[A] = StateT[IO, Connection, A]

}


object Query {

  def bind[C[_] <: JDBCColumn, Repr <: HList](cols: Columns[C, _, Repr], i: Int, con: Connection, ps: PreparedStatement, record: Repr): IO[Unit] = IO {
    @tailrec
    def loop(offs: Int, rec: HList): Unit = {
      rec match {
        case h :: tail =>
          val col = cols.columns(offs)
          col._2.bind(offs+i, h, con, ps)
          loop(offs+1, tail)
        case HNil => ()
      }
    }
    loop(0, record)
  }

  def get[C[_] <: JDBCColumn, Repr <: HList](cols: Columns[C, _, Repr], i: Int, rs: ResultSet) : IO[Repr] = IO {
    @tailrec
    def loop(offs: Int, l: HList) : HList = {
      if (offs < 0) l else {
        val (name,col) = cols.columns(offs)
        col.getByIndex(offs + i, rs) match {
          case None => throw new Error(s"Column ${name} is null")
          case Some(v) => loop(offs - 1, v :: l)
        }
      }
    }
    loop(cols.columns.length-1, HNil).asInstanceOf[Repr]
  }


  def rowsStream[A](open: DBIO[ResultSet]): Stream[DBIO, ResultSet] = {
    def nextLoop(rs: ResultSet): Stream[DBIO, ResultSet] =
      Stream.eval(IO(rs.next()).liftIO[DBIO]).flatMap {
        n => if (n) Stream(rs) ++ nextLoop(rs) else Stream.empty
      }

    Stream.bracket(open)(nextLoop, rs => IO(rs.close()).liftIO[DBIO])
  }

  def byPK[T, Repr <: HList, Key <: HList](table: JDBCTable.Aux[T, Repr, Key]): Stream[DBIO, Key] => Stream[DBIO, T] = {
    val select = JDBCSelect(table.name, table.all.columns.map(_._1),
      table.keys.columns.map(kc => EQ(kc._1)), Seq.empty, false)
    keys => {
      keys.flatMap { key =>
        rowsStream {
          StateT.inspectF { con =>
            for {
              ps <- IO {
                con.prepareStatement(JDBCPreparedQuery.asSQL(select, table.sqlMapping))
              }
              _ <- bind(table.keys, 1, con, ps, key)
              rs <- IO(ps.executeQuery())
            } yield rs
          }
        }.evalMap {
          rs => get(table.all, 1, rs).liftIO[DBIO]
        }.map(table.all.from)
      }
    }
  }
}



