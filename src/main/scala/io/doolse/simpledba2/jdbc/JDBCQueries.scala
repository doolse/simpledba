package io.doolse.simpledba2.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}

import cats.data.StateT
import cats.effect.IO
import fs2.{Pipe, Sink, Stream}
import io.doolse.simpledba2.{Columns, ReadQueries, WriteOp, WriteQueries}
import cats.effect.implicits._
import shapeless.{::, HList, HNil}
import cats.syntax.all._

import scala.annotation.tailrec

object JDBCQueries {

  def flush: Sink[JDBCIO, WriteOp] = writes => {
    writes.evalMap {
      case JDBCWriteOp(q, sqlMapping, binder) => StateT.inspectF { con =>
        for {
          ps <- IO { con.prepareStatement(JDBCPreparedQuery.asSQL(q, sqlMapping)) }
          _ <- binder(con, ps)
          _ <- IO(ps.execute())
        } yield ()
      }
    }
  }

  case class Bindable[C, KL](clauses: Seq[C], bind: KL => StateT[IO, (Int, Connection, PreparedStatement), Unit])

  def runBind(b: StateT[IO, (Int, Connection, PreparedStatement), Unit]): (Connection, PreparedStatement) => IO[Unit] =
    (con, ps) => b.runA(1, con, ps)

  def whereEQ[C[_] <: JDBCColumn, K, KL <: HList](columns: Columns[C, K, KL]): Bindable[JDBCWhereClause, KL] = {
    Bindable(columns.columns.map(c => EQ(c._1)), kl => bind(columns, kl))
  }

  case class JDBCWriteOp(query: JDBCPreparedQuery, config: JDBCSQLConfig, bind: (Connection, PreparedStatement) => IO[Unit]) extends WriteOp

  def writes[C[_] <: JDBCColumn, T, R <: HList, K <: HList](table: JDBCTable[C, T, R, K]) = new WriteQueries[JDBCIO, T] {

    val insertQuery = JDBCInsert(table.name, table.all.columns.map(_._1))

    override def insertAll: Pipe[JDBCIO, T, WriteOp] = _.map {
      t => JDBCWriteOp(insertQuery, table.sqlMapping, runBind(bind(table.all, table.all.to(t))))
    }

    override def updateAll: Pipe[JDBCIO, (T, T), WriteOp] = _.flatMap {
      case (o,n) =>
        val updateVals = bind(table.all, table.all.to(n))
        val keyVal = table.toKey(table.all.to(n))
        val whereClause = whereEQ(table.keys)
        Stream(JDBCWriteOp(JDBCUpdate(table.name, table.all.columns.map(_._1),
        whereClause.clauses), table.sqlMapping, runBind(updateVals.flatMap(_ => whereClause.bind(keyVal)))))
    }

    override def deleteAll: Pipe[JDBCIO, T, WriteOp] = ???
  }

  def apply[C[_] <: JDBCColumn, T, R <: HList, K <: HList](table: JDBCTable[C, T, R, K]) = new QueryBuilder[C, T, R, K, HNil, HNil,
    T, R](table,
    JDBCSelect(table.name, table.all.columns.map(_._1), Seq.empty, Seq.empty, limit = false), table.all,
    Columns.empty
  )

  case class QueryBuilder[C[_] <: JDBCColumn, T, R <: HList, K <: HList, W, WL <: HList, O, OL <: HList](table: JDBCTable[C, T, R, K],
                                               select: JDBCSelect,
                                               resultCols: Columns[C, O, OL], queryParams: Columns[C, W, WL])
  {
    def buildAll(implicit ev: WL =:= HNil): Stream[JDBCIO, O] = streamForQuery((_,_) => IO.pure())

    def build() : ReadQueries[JDBCIO, W, O] = new ReadQueries[JDBCIO, W, O] {
      override def find: Pipe[JDBCIO, W, O] = _.flatMap { key =>
        streamForQuery(runBind(bind(queryParams, queryParams.to(key))))
      }
    }

    def streamForQuery(f: (Connection, PreparedStatement) => IO[Unit]): Stream[JDBCIO, O] = {
      rowsStream {
        StateT.inspectF { con =>
          for {
            ps <- IO { con.prepareStatement(JDBCPreparedQuery.asSQL(select, table.sqlMapping)) }
            _ <- f(con, ps)
            rs <- IO(ps.executeQuery())
          } yield rs
        }
      }.evalMap {
        rs => get(resultCols, 1, rs).liftIO[JDBCIO]
      }.map(resultCols.from)
    }
  }

  def bind[C[_] <: JDBCColumn, R <: HList](cols: Columns[C, _, R], record: R):
  StateT[IO, (Int, Connection, PreparedStatement), Unit] = StateT {
    case (offs, con, ps) => IO {
      @tailrec
      def loop(i: Int, rec: HList): Int = {
        rec match {
          case h :: tail =>
            val (_, col) = cols.columns(i)
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
