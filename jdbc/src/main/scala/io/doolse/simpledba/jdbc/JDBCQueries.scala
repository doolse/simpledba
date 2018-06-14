package io.doolse.simpledba.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}

import cats.data.{Kleisli, StateT}
import cats.effect.IO
import cats.effect.implicits._
import fs2.{Pipe, Pure, Sink, Stream}
import io.doolse.simpledba._
import shapeless.ops.record.{Keys, SelectAll, ToMap}
import shapeless.{::, DepFn2, HList, HNil, Witness}
import shapeless.labelled._

import scala.annotation.tailrec

object JDBCQueries {

  def flush: Sink[JDBCIO, WriteOp] = writes => {
    Stream.eval(
    writes.evalMap {
      case JDBCWriteOp(q, config, binder) => StateT.inspectF { con =>
        def toSQL() = JDBCPreparedQuery.asSQL(q, config)
        for {
          ps <- IO {
            val sql = toSQL
            config.logPrepare(sql)
            con.prepareStatement(sql)
          }
          bv <- binder(con, ps).runA(1)
          _ <- IO {
            config.logBind(() => (toSQL(), bv))
            ps.execute()
          }
        } yield ()
      }
    }.compile.drain)
  }

  object BindNone
  case class BindOne[R, A, V](bind: V => (Seq[A], BindFunc[Option[BindLog]]))
  case class BindMany[R, A, V](bind: V => (Stream[Pure, (Seq[A], BindFunc[Option[BindLog]])]))
  case class BindTwo[R, A, B1, B2](f: B1, s: B2)

  trait BindableCombiner[B, B2] extends DepFn2[B, B2]

  trait BindStream[B, A, V] {
    def apply(b: B, v: V): Stream[Pure, (Seq[A], BindFunc[Seq[BindLog]])]
  }

  object BindStream {
    implicit def bindNone[R, A, Unit] = new BindStream[BindNone.type, A, Unit] {
      override def apply(b: BindNone.type, v: Unit) = Stream.empty
    }
    implicit def bindOne[R, A, V] = new BindStream[BindOne[R, A, V], A, V] {
      override def apply(b: BindOne[R, A, V], v: V): Stream[Pure, (Seq[A], BindFunc[Seq[BindLog]])] = {
        val (clauses,bind) = b.bind(v)
        Stream((clauses, bind.map(_.toSeq)))
      }
    }
  }

  object BindableCombiner
  {
    implicit def combineNoneLeft[B2] = new BindableCombiner[BindNone.type, B2] {
      type Out = B2

      override def apply(t: BindNone.type, u: B2) = u
    }
  }

  def colsEQ[C[_] <: JDBCColumn, R <: HList, K, KL <: HList](where: ColumnSubset[C, R, K, KL]): BindOne[R, JDBCWhereClause, K]
    = BindOne[R, JDBCWhereClause, K] { (k: K) =>
    val clauses = where.columns.map(c => EQ(JDBCColumnBinding(c)))
    (clauses, bindCols(where, where.iso.to(k)).map(v => Some(WhereBinding(v))))
  }


  case class QueryBuilder[C[_] <: JDBCColumn, T, R <: HList, K <: HList,
  W, B,
  O, OL <: HList]
  (table: JDBCTable[C, T, R, K], resultCols: Columns[C, O, OL], where: B,
    orderCols: Seq[(JDBCColumnBinding, Boolean)])
  {
    def orderBy[T <: Symbol](w: Witness, asc: Boolean)
               (implicit
                k: Keys.Aux[FieldType[w.T, Boolean] :: HNil, w.T :: HNil],
                toMap: ToMap.Aux[FieldType[w.T, Boolean] :: HNil, T, Boolean],
                sel: ColumnSubsetBuilder[R, w.T :: HNil])
      = orderWith(field[w.T](asc) :: HNil)

    def orderWith[OR <: HList, ORK <: HList, Syms <: Symbol](or: OR)(implicit keys: Keys.Aux[OR, ORK], toMap: ToMap.Aux[OR, Syms, Boolean],
                                          cssb: ColumnSubsetBuilder[R, ORK]) : QueryBuilder[C, T, R, K, W, B, O, OL] = {
      val m = toMap(or).map { case (s,b) => (s.name, b)}
      val actColMap = table.all.columns.toMap
      val cols = cssb.apply()._1.map(cn => (JDBCColumnBinding((cn, actColMap(cn))), m(cn)))
      copy(orderCols = cols)
    }

    def whereEQ[W2 <: HList](whereEQ: ColumnSubset[C, R, W2, W2])(implicit combiner: BindableCombiner[B, BindOne[R, JDBCWhereClause, W2]]) = {
      copy[C, T, R, K, W2, combiner.Out, O, OL](where = combiner(where, colsEQ(whereEQ)))
    }

    def build[W2](implicit c: AutoConvert[W2, W], binder: BindStream[B, JDBCWhereClause, W]) : W2 => Stream[JDBCIO, O] = {
      val baseSel = JDBCSelect(table.name, resultCols.columns.map(JDBCColumnBinding[C]),
        Seq.empty, orderCols, false)
        (w2 : W2) => {
          binder(where, c.apply(w2)).covary[JDBCIO].flatMap {
            case (wc, bind) => streamForQuery(baseSel.copy(where = wc), bind)
          }
        }
    }

    def streamForQuery(select: JDBCSelect, bind: BindFunc[Seq[BindLog]])
    : Stream[JDBCIO, O] = {
      rowsStream {
        StateT.inspectF { con =>
          def toSQL() = JDBCPreparedQuery.asSQL(select, table.config)
          for {
            ps <- IO {
              val sql = toSQL()
              table.config.logPrepare(sql)
              con.prepareStatement(sql)
            }
            bv <- bind(con, ps).runA(1)
            rs <- IO {
              table.config.logBind(() => (toSQL(), bv))
              ps.executeQuery()
            }
          } yield rs
        }
      }.evalMap {
        rs => getColRecord(resultCols, 1, rs).liftIO[JDBCIO]
      }.map(resultCols.iso.from)
    }
  }

  def bindCols[C[_] <: JDBCColumn, R <: HList](cols: ColumnRecord[C, R], record: R): BindFunc[List[Any]] = Kleisli {
    case (con, ps) =>
      StateT { offs =>
        IO {
          @tailrec
          def loop(i: Int, rec: HList, vals: List[Any]): (Int,List[Any]) = {
            rec match {
              case h :: tail =>
                val (_, col) = cols.columns(i)
                col.bind(offs + i, h.asInstanceOf[col.A], con, ps)
                loop(i + 1, tail, h :: vals)
              case HNil => (i+offs, vals)
            }
          }
          loop(0, record, Nil)
        }
      }
  }

  def getColRecord[C[_] <: JDBCColumn, R <: HList]
  (cols: Columns[C, _, R],
   i: Int, rs: ResultSet): IO[R] = IO {
    @tailrec
    def loop(offs: Int, l: HList): HList = {
      if (offs < 0) l else {
        val (name, col) = cols.columns(offs)
        col.read(offs + i, rs) match {
          case None => throw new Error(s"Column $name is null")
          case Some(v) => loop(offs - 1, v :: l)
        }
      }
    }

    loop(cols.columns.length - 1, HNil).asInstanceOf[R]
  }


  private def rowsStream[A](open: JDBCIO[ResultSet]): Stream[JDBCIO, ResultSet] = {
    def nextLoop(rs: ResultSet): Stream[JDBCIO, ResultSet] =
      Stream.eval(IO(rs.next()).liftIO[JDBCIO]).flatMap {
        n => if (n) Stream(rs) ++ nextLoop(rs) else Stream.empty
      }

    Stream.bracket(open)(nextLoop, rs => IO(rs.close()).liftIO[JDBCIO])
  }

}
