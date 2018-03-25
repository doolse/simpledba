package io.doolse.simpledba2.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}

import cats.data.StateT
import cats.effect.IO
import cats.effect.implicits._
import fs2.{Pipe, Sink, Stream}
import io.doolse.simpledba2._
import shapeless.ops.record.{Keys, SelectAll, ToMap}
import shapeless.{::, HList, HNil, Witness}
import shapeless.labelled._

import scala.annotation.tailrec

object JDBCQueries {

  def flush: Sink[JDBCIO, WriteOp] = writes => {
    Stream.eval(
    writes.evalMap {
      case JDBCWriteOp(q, sqlMapping, binder) => StateT.inspectF { con =>
        for {
          ps <- IO {
            val sql = JDBCPreparedQuery.asSQL(q, sqlMapping)
            println(sql)
            con.prepareStatement(sql)
          }
          _ <- binder(con, ps)
          _ <- IO(ps.execute())
        } yield ()
      }
    }.compile.drain)
  }

  case class Bindable[R, A, V](clauses: Seq[A], bind: V => StateT[IO, (Int, Connection, PreparedStatement), Unit])

  object Bindable
  {
    def empty[R, A] : Bindable[R, A, Unit] = Bindable(Seq.empty, _ => StateT.liftF(IO.pure()))
  }

  def runBind(b: StateT[IO, (Int, Connection, PreparedStatement), Unit]): (Connection, PreparedStatement) => IO[Unit] =
    (con, ps) => b.runA(1, con, ps)

  def colsEQ[C[_] <: JDBCColumn, R <: HList, K, KL <: HList](where: ColumnSubset[C, R, K, KL]): Bindable[R, JDBCWhereClause, K]
    = Bindable[R, JDBCWhereClause, K](where.columns.map(c => EQ(c._1)),
    k => bindUnsafe(where.columns, where.iso.to(k)))

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
        streamForQuery(jdbcSelect, runBind( queryParams.bind(c(key))))
      }
    }

    private def streamForQuery(select: JDBCSelect, f: (Connection, PreparedStatement) => IO[Unit]): Stream[JDBCIO, O] = {
      rowsStream {
        StateT.inspectF { con =>
          for {
            ps <- IO {
              val sql = JDBCPreparedQuery.asSQL(select, table.sqlMapping)
              println(sql)
              con.prepareStatement(sql)
            }
            _ <- f(con, ps)
            rs <- IO(ps.executeQuery())
          } yield rs
        }
      }.evalMap {
        rs => get(resultCols, 1, rs).liftIO[JDBCIO]
      }.map(resultCols.iso.from)
    }
  }

  def bindUnsafe[C[_] <: JDBCColumn, R <: HList](cols: Seq[(String, C[_])], record: R):
  StateT[IO, (Int, Connection, PreparedStatement), Unit] = StateT {
    case (offs, con, ps) => IO {
      @tailrec
      def loop(i: Int, rec: HList): Int = {
        rec match {
          case h :: tail =>
            val (_, col) = cols(i)
            col.bind(offs + i, h.asInstanceOf[col.A], con, ps)
            loop(i + 1, tail)
          case HNil => i
        }
      }

      ((loop(0, record)+offs, con, ps), ())
    }
  }

  def get[C[_] <: JDBCColumn, R <: HList]
  (cols: Columns[C, _, R],
   i: Int,
   rs: ResultSet): IO[R] = IO {
    @tailrec
    def loop(offs: Int, l: HList): HList = {
      if (offs < 0) l else {
        val (name, col) = cols.columns(offs)
        col.getByIndex(offs + i, rs) match {
          case None => throw new Error(s"Column ${name} is null")
          case Some(v) => loop(offs - 1, v :: l)
        }
      }
    }

    loop(cols.columns.length - 1, HNil).asInstanceOf[R]
  }


  def rowsStream[A](open: JDBCIO[ResultSet]): Stream[JDBCIO, ResultSet] = {
    def nextLoop(rs: ResultSet): Stream[JDBCIO, ResultSet] =
      Stream.eval(IO(rs.next()).liftIO[JDBCIO]).flatMap {
        n => if (n) Stream(rs) ++ nextLoop(rs) else Stream.empty
      }

    Stream.bracket(open)(nextLoop, rs => IO(rs.close()).liftIO[JDBCIO])
  }

}
