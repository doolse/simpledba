package io.doolse.simpledba.jdbc

import cats.data.{Kleisli, State}
import io.doolse.simpledba._
import io.doolse.simpledba.jdbc.BinOp.BinOp
import io.doolse.simpledba.jdbc.JDBCQueries._
import io.doolse.simpledba.jdbc.JDBCTable.TableRecord
import shapeless.labelled._
import shapeless.ops.hlist.{Length, Prepend, Split}
import shapeless.ops.record.{Keys, ToMap}
import shapeless.{::, DepFn2, HList, HNil, LabelledGeneric, Nat, Witness}

import scala.annotation.tailrec

case class JDBCMapper[C[_] <: JDBCColumn[_]](dialect: SQLDialect) {

  def record[R <: HList](implicit cr: ColumnRecord[C, Unit, R]): ColumnRecord[C, Unit, R] = cr

  def queries[S[_], F[_]](effect: JDBCEffect[S, F]): JDBCQueries[C, S, F] =
    JDBCQueries(effect, dialect)

  def mapped[T] = new RelationBuilder[T, C]

  class RelationBuilder[T, C[_] <: JDBCColumn[_]] {
    def embedded[GR <: HList, R <: HList](
        implicit
        gen: LabelledGeneric.Aux[T, GR],
        columns: ColumnBuilder.Aux[C, GR, R]
    ): ColumnBuilder.Aux[C, T, R] = new ColumnBuilder[C, T] {
      type Repr = R
      def apply() = columns().compose(Iso(gen.to, gen.from))
    }

    def table[GR <: HList, R <: HList](tableName: String)(
        implicit
        gen: LabelledGeneric.Aux[T, GR],
        allRelation: ColumnBuilder.Aux[C, GR, R]
    ): JDBCRelation[C, T, R] =
      JDBCRelation[C, T, R](
        tableName,
        allRelation()
          .compose(Iso(gen.to, gen.from))
      )
  }

}

case class BoundColumn(name: String, column: JDBCColumn[_], binder: ParameterBinder) {
  def namedColumn: NamedColumn = NamedColumn(name, column.columnType)
}

object BindValues extends ColumnMapper[JDBCColumn, String, BoundColumn] {
  override def apply[V](column: JDBCColumn[V], value: V, a: String): BoundColumn =
    BoundColumn(a, column, ParameterBinder(column.bindValue(value), value))
}

object BindUpdate extends ColumnCompare[JDBCColumn, String, BoundColumn] {
  override def apply[V](column: JDBCColumn[V],
                        value1: V,
                        value2: V,
                        a: String): Option[BoundColumn] = {
    column.bindUpdate(value1, value2).map(b => BoundColumn(a, column, ParameterBinder(b, value2)))
  }
}

trait BindValues {
  type Value
}

object BindNone extends BindValues {
  type Value = Unit
}

case class BindOne[R, A, V](bind: V => (Seq[A], BindFunc[Seq[BindLog]])) extends BindValues {
  type Value = V
}

case class BindMany[R, A, V](bind: V => (Seq[(Seq[A], BindFunc[Option[BindLog]])]))
    extends BindValues {
  type Value = V
}

trait BindableCombiner[B, B2] extends DepFn2[B, B2] {
  type Out <: BindValues
}

trait BindStream[B <: BindValues, A] {
  def apply(b: B)(v: b.Value): Seq[(Seq[A], BindFunc[Seq[BindLog]])]
}

object BindStream {
  implicit def bindNone[A] = new BindStream[BindNone.type, A] {
    override def apply(b: BindNone.type)(
        v: Unit
    ): Seq[(Seq[A], BindFunc[Seq[BindLog]])] = Seq()
  }

  implicit def bindOne[R, A, V0] = new BindStream[BindOne[R, A, V0], A] {
    type V = V0

    override def apply(
        b: BindOne[R, A, V]
    )(v: V): Seq[(Seq[A], BindFunc[Seq[BindLog]])] = {
      val (clauses, bind) = b.bind(v)
      Seq((clauses, bind.map(_.toSeq)))
    }
  }
}

object BindableCombiner {
  implicit def combineNoneLeft[B2 <: BindValues] = new BindableCombiner[BindNone.type, B2] {
    type Out = B2

    override def apply(t: BindNone.type, u: B2) = u
  }

  implicit def bindOneAndOne[R, A, B1 <: HList, B2 <: HList, B <: HList, B1L <: Nat](
      implicit prepend: Prepend.Aux[B1, B2, B],
      lenb1: Length.Aux[B1, B1L],
      split: Split.Aux[B, B1L, B1, B2]
  ) = new BindableCombiner[BindOne[R, A, B1], BindOne[R, A, B2]] {
    override type Out = BindOne[R, A, B]

    override def apply(t: BindOne[R, A, B1], u: BindOne[R, A, B2]): BindOne[R, A, B] =
      BindOne[R, A, B] { r =>
        val (b1, b2) = split(r)
        val bind1    = t.bind(b1)
        val bind2    = u.bind(b2)
        (bind1._1 ++ bind2._1, bind1._2.flatMap(bl1 => bind2._2.map(bl2 => bl1 ++ bl2)))
      }
  }

}

case class JDBCQueries[C[_] <: JDBCColumn[_], S[_], F[_]](E: JDBCEffect[S, F],
                                                          dialect: SQLDialect) {
  val S  = E.S
  val SM = S.SM

  def flushable: Flushable[S] =
    new Flushable[S] {
      def flush =
        writes =>
          S.eval(S.drain(SM.flatMap(writes) {
            case JDBCWriteOp(sql, binder) =>
              E.executePreparedQuery(sql, binder)
          }))
    }

  def writes(table: JDBCTable[C]): WriteQueries[S, F, table.Data] =
    new WriteQueries[S, F, table.Data] {
      def S = E.S

      override def insertAll =
        st =>
          SM.map(st) { t =>
            val allColumns    = table.allColumns
            val columnBinders = allColumns.mapRecord(allColumns.iso.to(t), BindValues)
            val insertQuery =
              dialect.querySQL(JDBCInsert(table.name, columnBinders.map(columnExpression)))
            JDBCWriteOp(
              insertQuery,
              bindParameters(columnBinders.map(_.binder)).map(v => Seq(ValueLog(v)))
            )
        }

      override def updateAll =
        st =>
          SM.flatMap(st) {
            case (o, n) =>
              val allColumns = table.allColumns
              val oldRec     = allColumns.iso.to(o)
              val oldKey     = table.toKey(oldRec)
              val newRec     = allColumns.iso.to(n)
              val keyVal     = table.toKey(newRec)
              if (oldKey != keyVal) {
                S.append(S.emit(deleteWriteOp(oldKey)), insert(n))
              } else {
                val (whereClauses, bind) = colsOp(BinOp.EQ, table.keyColumns).bind(keyVal)
                val updateColumns        = allColumns.compareRecords(oldRec, newRec, BindUpdate)
                if (updateColumns.isEmpty) S.empty
                else {
                  val binder = for {
                    updateVals <- bindParameters(updateColumns.map(_.binder))
                    wc         <- bind
                  } yield Seq(ValueLog(updateVals)) ++ wc
                  val updateSQL = dialect.querySQL(
                    JDBCUpdate(table.name, updateColumns.map(columnExpression), whereClauses))
                  S.emit(
                    JDBCWriteOp(
                      updateSQL,
                      binder
                    )
                  )
                }
              }
        }

      override def deleteAll =
        st => SM.map(st)(t => deleteWriteOp(table.toKey(table.allColumns.iso.to(t))))

      private def deleteWriteOp(k: table.KeyList): JDBCWriteOp = {
        val (whereClauses, bindVals) = colsOp(BinOp.EQ, table.keyColumns).bind(k)
        val deleteSQL                = dialect.querySQL(JDBCDelete(table.name, whereClauses))
        JDBCWriteOp(deleteSQL, bindVals.map(_.toList))
      }

    }

  def selectFrom(table: JDBCTable[C]) =
    new QueryBuilder[S, F, C, table.DataRec, BindNone.type, HNil, HNil](
      table,
      dialect,
      E,
      ColumnRecord.empty,
      identity,
      BindNone,
      Seq.empty
    )

  def deleteFrom(table: JDBCTable[C]) =
    new DeleteBuilder[S, C, F, table.DataRec, BindNone.type](E.S, table, dialect, BindNone)

  def query(table: JDBCTable[C]) =
    new QueryBuilder[S, F, C, table.DataRec, BindNone.type, table.DataRec, table.Data](
      table,
      dialect,
      E,
      toProjection(table.allColumns),
      table.allColumns.iso.from,
      BindNone,
      Seq.empty
    )

  def byPK[K2](table: JDBCTable[C])(
      implicit c: AutoConvert[K2, table.KeyList]): K2 => S[table.Data] = {
    implicit val kn = table.keySubset
    query(table).where(table.keyNames, BinOp.EQ).build[K2]
  }

  def allRows(table: JDBCTable[C]): S[table.Data] =
    query(table).build[Unit].apply()

  def queryRawSQL[Params <: HList, OutRec <: HList](
      sql: String,
      cr: ColumnRecord[JDBCColumn, String, Params],
      outRec: ColumnRecord[JDBCColumn, Any, OutRec]
  ): Params => S[OutRec] =
    params => {
      val bindFunc = bindParameters(cr.mapRecord(params, BindValues).map(_.binder))
        .map(l => Seq(ValueLog(l): BindLog))
      S.evalMap(E.executeResultSet(sql, bindFunc)) { rs =>
        E.resultSetRecord(outRec, 1, rs)
      }
    }

  def rawSQL(sql: String): WriteOp = {
    JDBCWriteOp(sql, Kleisli.pure(Seq.empty))
  }

  def rawSQLStream(
      sql: S[String]
  ): S[WriteOp] = {
    SM.map(sql)(rawSQL)
  }

}

object JDBCQueries {

  case class ParameterBinder(param: ParamBinder, value: Any)

  def colsOp[C[_] <: JDBCColumn[_], R <: HList, K <: HList](
      op: BinOp,
      where: ColumnSubset[C, R, K]
  ): BindOne[R, JDBCWhereClause, K] = BindOne[R, JDBCWhereClause, K] { k: K =>
    val columnExprs = where.mapRecord(k, BindValues)
    val colWhere = columnExprs.map { bc =>
      BinClause(ColumnReference(bc.namedColumn), op, Parameter(bc.column.columnType))
    }
    (colWhere, bindParameters(columnExprs.map(_.binder)).map(v => Seq(WhereLog(v))))
  }

  case class DeleteBuilder[S[_], C[_] <: JDBCColumn[_], F[_], DataRec <: HList, B <: BindValues](
      S: Streamable[S, F],
      table: TableRecord[C, DataRec],
      dialect: SQLDialect,
      whereClause: B
  ) {
    def where[W2 <: HList, ColNames <: HList](cols: Cols[ColNames], op: BinOp)(
        implicit css: ColumnSubsetBuilder.Aux[DataRec, ColNames, W2],
        combiner: BindableCombiner[B, BindOne[DataRec, JDBCWhereClause, W2]]
    ) = {
      copy[S, C, F, DataRec, combiner.Out](
        whereClause = combiner(whereClause, colsOp(op, table.cols(cols)))
      )
    }

    def where[W2 <: HList](col: Witness, op: BinOp)(
        implicit cols: ColumnSubsetBuilder.Aux[DataRec, col.T :: HNil, W2],
        combiner: BindableCombiner[B, BindOne[DataRec, JDBCWhereClause, W2]]
    ) = {
      copy[S, C, F, DataRec, combiner.Out](
        whereClause = combiner(whereClause, colsOp(op, table.cols(Cols(col))))
      )
    }

    def build[W2](
        implicit b: BindStream[B, JDBCWhereClause],
        c: AutoConvert[W2, whereClause.Value]
    ): W2 => S[WriteOp] = w => {
      S.SM.map(S.emits(b.apply(whereClause)(w)))(a => {
        val deleteSQL = dialect.querySQL(JDBCDelete(table.name, a._1))
        JDBCWriteOp(deleteSQL, a._2)
      })
    }
  }

  case class QueryBuilder[S[_],
                          F[_],
                          C[_] <: JDBCColumn[_],
                          DataRec <: HList,
                          B <: BindValues,
                          OutRec <: HList,
                          Out](
      table: TableRecord[C, DataRec],
      dialect: SQLDialect,
      E: JDBCEffect[S, F],
      projections: ColumnRecord[C, SQLProjection, OutRec],
      mapOut: OutRec => Out,
      where: B,
      orderCols: Seq[(NamedColumn, Boolean)]
  ) {

    val S  = E.S
    val SM = S.SM

    def count(
        implicit intCol: C[Int]
    ): QueryBuilder[S, F, C, DataRec, B, Int :: OutRec, Int :: OutRec] = {
      val newProjection = ColumnRecord.prepend(
        ColumnRecord[C, SQLProjection, Int :: HNil](
          Seq(SQLProjection(intCol.columnType, Aggregate(AggregateOp.Count, None)) -> intCol)
        ),
        projections
      )
      copy[S, F, C, DataRec, B, Int :: OutRec, Int :: OutRec](
        projections = newProjection,
        mapOut = identity
      )
    }

    def cols[CT <: HList, NewOutRec <: HList, ColNames <: HList](cols: Cols[ColNames])(
        implicit c: ColumnSubsetBuilder.Aux[DataRec, ColNames, CT],
        prepend: Prepend.Aux[CT, OutRec, NewOutRec]
    ): QueryBuilder[S, F, C, DataRec, B, NewOutRec, NewOutRec] = {
      val newProjection =
        ColumnRecord.prepend(toProjection(table.allColumns.subset(c)), projections)
      copy[S, F, C, DataRec, B, prepend.Out, prepend.Out](projections = newProjection,
                                                          mapOut = identity)
    }

    def orderBy[T <: Symbol](w: Witness, asc: Boolean)(
        implicit
        k: Keys.Aux[FieldType[w.T, Boolean] :: HNil, w.T :: HNil],
        toMap: ToMap.Aux[FieldType[w.T, Boolean] :: HNil, T, Boolean],
        sel: ColumnSubsetBuilder[DataRec, w.T :: HNil]
    ) = orderWith(field[w.T](asc) :: HNil)

    def orderWith[OR <: HList, ORK <: HList, Syms <: Symbol](or: OR)(
        implicit keys: Keys.Aux[OR, ORK],
        toMap: ToMap.Aux[OR, Syms, Boolean],
        cssb: ColumnSubsetBuilder[DataRec, ORK]
    ): QueryBuilder[S, F, C, DataRec, B, OutRec, Out] = {
      val m         = toMap(or).map { case (s, b) => (s.name, b) }
      val actColMap = table.allColumns.columns.toMap
      val cols      = cssb.apply()._1.map(cn => (NamedColumn((cn, actColMap(cn))), m(cn)))
      copy(orderCols = cols)
    }

    def where[W2 <: HList](whereCol: Witness, binOp: BinOp)(
        implicit csb: ColumnSubsetBuilder.Aux[DataRec, whereCol.T :: HNil, W2],
        combiner: BindableCombiner[B, BindOne[DataRec, JDBCWhereClause, W2]]
    ) = {
      copy[S, F, C, DataRec, combiner.Out, OutRec, Out](
        where = combiner(where, colsOp(binOp, table.cols(Cols(whereCol))))
      )
    }

    def where[W2 <: HList, ColNames <: HList](whereCols: Cols[ColNames], binOp: BinOp)(
        implicit
        css: ColumnSubsetBuilder.Aux[DataRec, ColNames, W2],
        combiner: BindableCombiner[B, BindOne[DataRec, JDBCWhereClause, W2]]
    ) = {
      copy[S, F, C, DataRec, combiner.Out, OutRec, Out](
        where = combiner(where, colsOp(binOp, table.cols(whereCols)))
      )
    }

    def buildAs[In, Out2](
        implicit c: AutoConvert[In, where.Value],
        cout: AutoConvert[Out, Out2],
        binder: BindStream[B, JDBCWhereClause]
    ): In => S[Out2] = build[In].andThen(o => SM.map(o)(cout.apply))

    def build[In](
        implicit c: AutoConvert[In, where.Value],
        binder: BindStream[B, JDBCWhereClause]
    ): In => S[Out] = {
      val baseSel =
        JDBCSelect(table.name, projections.columns.map(_._1), Seq.empty, orderCols, false)
      w2: In =>
        {
          SM.flatMap(S.emits(binder(where)(c.apply(w2)))) {
            case (wc, bind) =>
              val selSQL = dialect.querySQL(baseSel.copy(where = wc))
              SM.map(E.streamForQuery(selSQL, bind, projections))(mapOut)
          }
        }
    }
  }

  def bindParameters(params: Seq[ParameterBinder]): BindFunc[List[Any]] = Kleisli {
    case (con, ps) =>
      State { offs =>
        @tailrec
        def loop(i: Int, outvals: List[Any]): (Int, List[Any]) = {
          if (i < params.length) {
            val c = params(i)
            c.param(i + offs, con, ps)
            loop(i + 1, c.value :: outvals)
          } else (i + offs, outvals)
        }

        loop(0, Nil)
      }
  }

  def columnExpression[A](bound: BoundColumn): ColumnExpression =
    ColumnExpression(bound.namedColumn, Parameter(bound.column.columnType))

  def toProjection[C[_] <: JDBCColumn[_], R <: HList](
      cols: ColumnRecord[C, String, R]
  ): ColumnRecord[C, SQLProjection, R] = {
    ColumnRecord {
      cols.columns.map {
        case (name, col) =>
          (SQLProjection(col.columnType, ColumnReference(NamedColumn(name, col.columnType))), col)
      }
    }
  }
}
