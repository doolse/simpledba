package io.doolse.simpledba.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}

import cats.data.{Kleisli, StateT}
import cats.effect.IO
import cats.effect.implicits._
import fs2.{Pure, Sink, Stream}
import io.doolse.simpledba.{jdbc, _}
import io.doolse.simpledba.jdbc.BinOp.BinOp
import io.doolse.simpledba.jdbc.JDBCTable.TableRecord
import shapeless.labelled._
import shapeless.ops.hlist.{Length, Prepend, Split, Take}
import shapeless.ops.record.{Keys, ToMap}
import shapeless.{::, DepFn2, HList, HNil, Nat, Witness}

import scala.annotation.tailrec
import scala.collection.mutable

object JDBCQueries {

  def flush: Sink[JDBCIO, WriteOp] = writes => {
    Stream.eval(
      writes.evalMap {
        case JDBCWriteOp(q, config, binder) => StateT.inspectF { con =>
          def toSQL() = config.queryToSQL(q)

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

  case class ParameterBinder(param: ParamBinder, value: Any)

  trait BindValues {
    type Value
  }

  object BindNone extends BindValues {
    type Value = Unit
  }

  case class BindOne[R, A, V](bind: V => (Seq[A], BindFunc[Seq[BindLog]])) extends BindValues {
    type Value = V
  }

  case class BindMany[R, A, V](bind: V => (Stream[Pure, (Seq[A], BindFunc[Option[BindLog]])])) extends BindValues {
    type Value = V
  }

  trait BindableCombiner[B, B2] extends DepFn2[B, B2] {
    type Out <: BindValues
  }

  trait BindStream[B <: BindValues, A] {
    def apply(b: B)(v: b.Value): Stream[Pure, (Seq[A], BindFunc[Seq[BindLog]])]
  }

  object BindStream {
    implicit def bindNone[A] = new BindStream[BindNone.type, A] {
      override def apply(b: BindNone.type)(v: Unit): Stream[Pure, (Seq[A], BindFunc[Seq[BindLog]])] = Stream.empty
    }

    implicit def bindOne[R, A, V0] = new BindStream[BindOne[R, A, V0], A] {
      type V = V0

      override def apply(b: BindOne[R, A, V])(v: V): Stream[Pure, (Seq[A], BindFunc[Seq[BindLog]])] = {
        val (clauses, bind) = b.bind(v)
        Stream((clauses, bind.map(_.toSeq)))
      }
    }
  }

  object BindableCombiner {
    implicit def combineNoneLeft[B2 <: BindValues] = new BindableCombiner[BindNone.type, B2] {
      type Out = B2

      override def apply(t: BindNone.type, u: B2) = u
    }

    implicit def bindOneAndOne[R, A, B1 <: HList,
      B2 <: HList, B <: HList, B1L <: Nat](implicit prepend: Prepend.Aux[B1, B2, B], lenb1: Length.Aux[B1, B1L],
                                           split: Split.Aux[B, B1L, B1, B2]) = new BindableCombiner[BindOne[R, A, B1], BindOne[R, A, B2]] {
      override type Out = BindOne[R, A, B]

      override def apply(t: BindOne[R, A, B1], u: BindOne[R, A, B2]): BindOne[R, A, B] =
        BindOne[R, A, B] { r =>
          val (b1, b2) = split(r)
          val bind1 = t.bind(b1)
          val bind2 = u.bind(b2)
          (bind1._1 ++ bind2._1, bind1._2.flatMap(bl1 => bind2._2.map(bl2 => bl1 ++ bl2)))
        }
    }

  }

  def colsOp[C[_] <: JDBCColumn, R <: HList, K, KL <: HList](op: BinOp, where: ColumnSubset[C, R, K, KL]): BindOne[R, JDBCWhereClause, K]
  = BindOne[R, JDBCWhereClause, K] { k: K =>
    val columnExprs = bindValues(where, where.iso.to(k)).columns
    val colWhere = columnExprs.map {
      case ((_, name), col) => BinClause(ColumnReference(NamedColumn(name, col.columnType)), op, Parameter(col.columnType))
    }
    (colWhere, bindParameters(columnExprs.map(_._1._1)).map(v => Seq(WhereLog(v))))
  }

  case class DeleteBuilder[C[_] <: JDBCColumn, DataRec <: HList, B <: BindValues]
  (table: TableRecord[C, DataRec], whereClause: B) {
    def where[W2 <: HList, ColNames <: HList]
    (cols: Cols[ColNames], op: BinOp)(
      implicit css: ColumnSubsetBuilder.Aux[DataRec, ColNames, W2],
      combiner: BindableCombiner[B, BindOne[DataRec, JDBCWhereClause, W2]]) = {
      copy[C, DataRec, combiner.Out](whereClause = combiner(whereClause, colsOp(op, table.cols(cols))))
    }

    def where[W2 <: HList]
    (col: Witness, op: BinOp)
    (implicit cols: ColumnSubsetBuilder.Aux[DataRec, col.T :: HNil, W2],
     combiner: BindableCombiner[B, BindOne[DataRec, JDBCWhereClause, W2]]) = {
      copy[C, DataRec, combiner.Out](whereClause = combiner(whereClause, colsOp(op, table.cols(Cols(col)))))
    }

    def build[W2](implicit b: BindStream[B, JDBCWhereClause], c: AutoConvert[W2, whereClause.Value]): W2 => Stream[JDBCIO, WriteOp] = w => {
      b.apply(whereClause)(w).map(a => JDBCWriteOp(JDBCDelete(table.name, a._1), table.config, a._2))
    }
  }


  case class QueryBuilder[C[_] <: JDBCColumn, DataRec <: HList, B <: BindValues, OutRec <: HList, Out]
  (table: TableRecord[C, DataRec], projections: ColumnRecord[C, SQLProjection, OutRec], mapOut: OutRec => Out, where: B,
   orderCols: Seq[(NamedColumn, Boolean)]) {

    def count(implicit intCol: C[Int]): QueryBuilder[C, DataRec, B, Int :: OutRec, Int :: OutRec] = {
      val newProjection = ColumnRecord.prepend(ColumnRecord[C, SQLProjection, Int :: HNil](Seq(
        SQLProjection(intCol.columnType, Aggregate(AggregateOp.Count, None)) -> intCol)), projections)
      copy[C, DataRec, B, Int :: OutRec, Int :: OutRec](projections = newProjection, mapOut = identity)
    }

    def cols[CT <: HList, NewOutRec <: HList, ColNames <: HList]
    (cols: Cols[ColNames])
    (implicit c: ColumnSubsetBuilder.Aux[DataRec, ColNames, CT], prepend: Prepend.Aux[CT, OutRec, NewOutRec]):
    QueryBuilder[C, DataRec, B, NewOutRec, NewOutRec] = {
      val newProjection = ColumnRecord.prepend(toProjection(table.allColumns.subset(c)._1), projections)
      copy[C, DataRec, B, prepend.Out, prepend.Out](projections = newProjection, mapOut = identity)
    }


    def orderBy[T <: Symbol](w: Witness, asc: Boolean)
                            (implicit
                             k: Keys.Aux[FieldType[w.T, Boolean] :: HNil, w.T :: HNil],
                             toMap: ToMap.Aux[FieldType[w.T, Boolean] :: HNil, T, Boolean],
                             sel: ColumnSubsetBuilder[DataRec, w.T :: HNil])
    = orderWith(field[w.T](asc) :: HNil)

    def orderWith[OR <: HList, ORK <: HList, Syms <: Symbol]
    (or: OR)
    (implicit keys: Keys.Aux[OR, ORK], toMap: ToMap.Aux[OR, Syms, Boolean],
     cssb: ColumnSubsetBuilder[DataRec, ORK]): QueryBuilder[C, DataRec, B, OutRec, Out] = {
      val m = toMap(or).map { case (s, b) => (s.name, b) }
      val actColMap = table.allColumns.columns.toMap
      val cols = cssb.apply()._1.map(cn => (NamedColumn((cn, actColMap(cn))), m(cn)))
      copy(orderCols = cols)
    }

    def where[W2 <: HList]
    (whereCol: Witness, binOp: BinOp)
    (implicit csb: ColumnSubsetBuilder.Aux[DataRec, whereCol.T :: HNil, W2],
     combiner: BindableCombiner[B, BindOne[DataRec, JDBCWhereClause, W2]]) = {
      copy[C, DataRec, combiner.Out, OutRec, Out](where = combiner(where, colsOp(binOp, table.cols(Cols(whereCol)))))
    }


    def where[W2 <: HList, ColNames <: HList]
    (whereCols: Cols[ColNames], binOp: BinOp)
    (implicit
     css: ColumnSubsetBuilder.Aux[DataRec, ColNames, W2],
     combiner: BindableCombiner[B, BindOne[DataRec, JDBCWhereClause, W2]]) = {
      copy[C, DataRec, combiner.Out, OutRec, Out](where = combiner(where, colsOp(binOp, table.cols(whereCols))))
    }

    def buildAs[In, Out2]
    (implicit c: AutoConvert[In, where.Value], cout: AutoConvert[Out, Out2],
     binder: BindStream[B, JDBCWhereClause]): In => Stream[JDBCIO, Out2] = build[In].andThen(_.map(cout.apply))

    def build[In]
    (implicit c: AutoConvert[In, where.Value],
     binder: BindStream[B, JDBCWhereClause]): In => Stream[JDBCIO, Out] = {
      val baseSel = JDBCSelect(table.name, projections.columns.map(_._1), Seq.empty, orderCols, false)
      w2: In => {
        binder(where)(c.apply(w2)).covary[JDBCIO].flatMap {
          case (wc, bind) => streamForQuery(table.config, baseSel.copy(where = wc), bind, projections).map(mapOut)
        }
      }
    }
  }

  def getColRecord[C[_] <: JDBCColumn, R <: HList]
  (cols: ColumnRecord[C, _, R],
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


  def rowsStream[A](open: JDBCIO[ResultSet]): Stream[JDBCIO, ResultSet] = {
    def nextLoop(rs: ResultSet): Stream[JDBCIO, ResultSet] =
      Stream.eval(IO(rs.next()).liftIO[JDBCIO]).flatMap {
        n => if (n) Stream(rs) ++ nextLoop(rs) else Stream.empty
      }

    Stream.bracket(open)(nextLoop, rs => IO(rs.close()).liftIO[JDBCIO])
  }

  def bindValues[C[_] <: JDBCColumn, R <: HList, A](cols: ColumnRecord[C, A, R], record: R): ColumnRecord[C, (ParameterBinder, A), R] = {
    val binds = mutable.Buffer[((ParameterBinder, A), C[_])]()

    @tailrec
    def loop(i: Int, rec: HList): Unit = {
      rec match {
        case h :: tail =>
          val (a, col) = cols.columns(i)
          val v = h.asInstanceOf[col.A]
          val binder = ParameterBinder(col.bindValue(v), v)
          binds += Tuple2((binder, a), col)
          loop(i + 1, tail)
        case HNil => ()
      }
    }

    loop(0, record)
    ColumnRecord(binds)
  }

  def bindUpdate[C[_] <: JDBCColumn, R <: HList, A](cols: ColumnRecord[C, A, R], existing: R, newrec: R): ColumnRecord[C, (ParameterBinder, A), R] = {
    val binds = mutable.Buffer[((ParameterBinder, A), C[_])]()

    @tailrec
    def loop(i: Int, ex: HList, nr: HList): Unit = {
      (ex, nr) match {
        case (hex :: tailold, hnr :: tailnew) =>
          val (a, col) = cols.columns(i)
          val oldv = hex.asInstanceOf[col.A]
          val newv = hnr.asInstanceOf[col.A]
          col.bindUpdate(oldv, newv).foreach {
            case binder =>
              binds += Tuple2((ParameterBinder(binder, newv), a), col)
          }
          loop(i + 1, tailold, tailnew)
        case _ => ()
      }
    }

    loop(0, existing, newrec)
    ColumnRecord(binds)
  }


  def bindParameters(params: Seq[ParameterBinder]): BindFunc[List[Any]] = Kleisli {
    case (con, ps) => StateT { offs =>
      @tailrec
      def loop(i: Int, outvals: List[Any]): (Int, List[Any]) = {
        if (i < params.length) {
          val c = params(i)
          c.param(i + offs, con, ps)
          loop(i + 1, c.value :: outvals)
        }
        else (i + offs, outvals)
      }

      IO {
        loop(0, Nil)
      }
    }
  }

  def columnExpressions[C[_] <: JDBCColumn, R <: HList, A](colParams: ColumnRecord[C, (A, String), R]): Seq[ColumnExpression] =
    colParams.columns.map {
      case ((_, name), col) => ColumnExpression(NamedColumn(name, col.columnType), Parameter(col.columnType))
    }


  def prepareAndQuery(config: JDBCConfig, query: JDBCPreparedQuery, bind: BindFunc[Seq[BindLog]],
                      mkStatement: (Connection, String) => PreparedStatement =
                      (con, sql) => con.prepareStatement(sql), mkResultSet: PreparedStatement => ResultSet = _.executeQuery()): Stream[JDBCIO, ResultSet] = {
    rowsStream {
      StateT.inspectF { con =>
        val sql = config.queryToSQL(query)
        for {
          ps <- IO {
            config.logPrepare(sql)
            mkStatement(con, sql)
          }
          bv <- bind(con, ps).runA(1)
          rs <- IO {
            config.logBind(() => (sql, bv))
            mkResultSet(ps)
          }
        } yield rs
      }
    }
  }

  def streamForQuery[C[_] <: JDBCColumn, Out <: HList]
  (config: JDBCConfig,
   query: JDBCPreparedQuery, bind: BindFunc[Seq[BindLog]], resultCols: ColumnRecord[C, _, Out])
  : Stream[JDBCIO, Out] = {
    prepareAndQuery(config, query, bind).evalMap {
      rs => getColRecord(resultCols, 1, rs).liftIO[JDBCIO]
    }
  }

  def toProjection[C[_] <: JDBCColumn, R <: HList](cols: ColumnRecord[C, String, R]): ColumnRecord[C, SQLProjection, R] = {
    ColumnRecord {
      cols.columns.map {
        case (name, col) => (SQLProjection(col.columnType, ColumnReference(NamedColumn(name, col.columnType))), col)
      }
    }
  }

  def queryRawSQL[C[_] <: JDBCColumn, Params <: HList, OutRec <: HList]
  (sql: String, cr: ColumnRecord[C, _, Params],
   outRec: ColumnRecord[C, _, OutRec])(implicit c: JDBCConfig): Params => Stream[JDBCIO, OutRec] = params => {
    val bindFunc = bindParameters(bindValues(cr, params).columns.map(_._1._1)).map(l => Seq(ValueLog(l): BindLog))
    prepareAndQuery(c, JDBCRawSQL(sql), bindFunc).evalMap { rs =>
      getColRecord(outRec, 1, rs).liftIO[JDBCIO]
    }
  }

}
