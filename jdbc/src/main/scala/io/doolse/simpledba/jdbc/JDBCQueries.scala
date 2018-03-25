package io.doolse.simpledba.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}

import cats.data.{Kleisli, StateT}
import cats.effect.IO
import cats.effect.implicits._
import fs2.{Pipe, Sink, Stream}
import io.doolse.simpledba._
import shapeless.ops.record.{Keys, SelectAll, ToMap}
import shapeless.{::, HList, HNil, Witness}
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

  case class Bindable[R, A, V](clauses: Seq[A], bind: V => BindFunc[Option[BindLog]])

  object Bindable
  {
    def empty[R, A] : Bindable[R, A, Unit] = Bindable(Seq.empty,
      _ => Kleisli.pure(None))
  }

  def colsEQ[C[_] <: JDBCColumn, R <: HList, K, KL <: HList](where: ColumnSubset[C, R, K, KL]): Bindable[R, JDBCWhereClause, K]
    = Bindable[R, JDBCWhereClause, K](where.columns.map(c => EQ(c._1)),
    k => bindCols(where, where.iso.to(k)).map(v => Some(WhereBinding(v))))

  case class QueryBuilder[C[_] <: JDBCColumn, T, R <: HList, K <: HList, W, O, OL <: HList](table: JDBCTable[C, T, R, K],
                                               resultCols: Columns[C, O, OL], queryParams: Bindable[R, JDBCWhereClause, W],
    orderCols: Seq[(String, Boolean)])
  {
    def orderBy[T <: Symbol](w: Witness, asc: Boolean)
               (implicit
                k: Keys.Aux[FieldType[w.T, Boolean] :: HNil, w.T :: HNil],
                toMap: ToMap.Aux[FieldType[w.T, Boolean] :: HNil, T, Boolean],
                sel: ColumnSubsetBuilder[R, w.T :: HNil])
      = orderWith(field[w.T](asc) :: HNil)

    def orderWith[OR <: HList, ORK <: HList, T <: Symbol](or: OR)(implicit keys: Keys.Aux[OR, ORK], toMap: ToMap.Aux[OR, T, Boolean],
                                          cssb: ColumnSubsetBuilder[R, ORK]) = {
      val m = toMap(or).map { case (s,b) => (s.name, b)}
      val cols = cssb.apply()._1.map(cn => (cn, m(cn)))
      copy(orderCols = cols)
    }

    def whereEQ[W2 <: HList](where: ColumnSubset[C, R, W2, W2]) = {
      copy(queryParams = colsEQ(where))
    }

    private def jdbcSelect = JDBCSelect(table.name, resultCols.columns.map(_._1),
      queryParams.clauses, orderCols, false)

    def build[W2](implicit c: AutoConvert[W2, W]) : ReadQueries[JDBCIO, W2, O] = new ReadQueries[JDBCIO, W2, O] {
      override def find: Pipe[JDBCIO, W2, O] = _.flatMap { key =>
        streamForQuery(jdbcSelect, queryParams.bind(c(key)).map(_.toSeq))
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
        col.getByIndex(offs + i, rs) match {
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
