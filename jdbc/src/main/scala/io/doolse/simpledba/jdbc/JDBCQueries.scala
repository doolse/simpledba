package io.doolse.simpledba.jdbc

import java.sql.{Connection, PreparedStatement}

import cats.data.NonEmptyList
import io.doolse.simpledba._
import io.doolse.simpledba.jdbc.AggregateOp.AggregateOp
import io.doolse.simpledba.jdbc.BinOp.BinOp
import io.doolse.simpledba.jdbc.JDBCQueries._
import io.doolse.simpledba.jdbc.JDBCTable.TableRecord
import shapeless.labelled._
import shapeless.ops.hlist.{Length, Prepend, Split}
import shapeless.ops.record.{Keys, Selector, ToMap}
import shapeless.{::, Generic, HList, HNil, LabelledGeneric, Nat, Witness}

import scala.annotation.tailrec
import scala.collection.mutable

case class JDBCMapper[C[A] <: JDBCColumn[A]](dialect: SQLDialect) {

  def queries[S[_], F[_]](effect: JDBCEffect[S, F]): JDBCQueries[C, S, F] =
    JDBCQueries(effect, dialect)

  def mapped[T] = new RelationBuilder[T, C]

  class RelationBuilder[T, C[A] <: JDBCColumn[A]] {
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

case class BoundColumn(name: String, column: JDBCColumn[_], binder: BoundValue) {
  def namedColumn: NamedColumn = NamedColumn(name, column.columnType)
}

object BindValues extends ColumnMapper[JDBCColumn, Any, BoundValue] {
  override def apply[V](column: JDBCColumn[V], value: V, a: Any): BoundValue =
    column.bindValue(value)
}

object BindNamedValues extends ColumnMapper[JDBCColumn, String, BoundColumn] {
  override def apply[V](column: JDBCColumn[V], value: V, a: String): BoundColumn =
    BoundColumn(a, column, column.bindValue(value))
}

object BindUpdate extends ColumnCompare[JDBCColumn, String, BoundColumn] {
  override def apply[V](column: JDBCColumn[V],
                        value1: V,
                        value2: V,
                        a: String): Option[BoundColumn] = {
    column.bindUpdate(value1, value2).map(b => BoundColumn(a, column, b))
  }
}

case class JDBCQueries[C[A] <: JDBCColumn[A], S[_], F[_]](effect: JDBCEffect[S, F],
                                                                       dialect: SQLDialect) {
  private val S = effect.S
  private val M = effect.M

  def flush(writes: S[JDBCWriteOp]): F[Unit] = effect.flush(writes)

  def writes(table: JDBCTable[C]): WriteQueries[S, F, JDBCWriteOp, table.Data] =
    new WriteQueries[S, F, JDBCWriteOp, table.Data] {
      override def S = effect.S
      def M = effect.M

      override def insertAll =
        st =>
          S.mapS(st) { t =>
            val allColumns    = table.allColumns
            val columnBinders = allColumns.mapRecord(allColumns.iso.to(t), BindNamedValues)
            val insertQuery =
              dialect.querySQL(JDBCInsert(table.name, columnBinders.map(columnExpression)))
            JDBCWriteOp(
              insertQuery,
              bindParameters(columnBinders.map(_.binder))
            )
        }

      override def updateAll =
        st =>
          S.flatMapS(st) {
            case (o, n) =>
              val allColumns = table.allColumns
              val oldRec     = allColumns.iso.to(o)
              val oldKey     = table.toKey(oldRec)
              val newRec     = allColumns.iso.to(n)
              val keyVal     = table.toKey(newRec)
              if (oldKey != keyVal) {
                S.append(S.emit(deleteWriteOp(oldKey)), insert(n))
              } else {
                val whereClauses  = colsOp(BinOp.EQ, table.keyColumns).apply(keyVal)
                val updateColumns = allColumns.compareRecords(oldRec, newRec, BindUpdate)
                if (updateColumns.isEmpty) S.empty
                else {
                  val binder = bindParameters(updateColumns.map(_.binder) ++ whereClauses.map(_._2))
                  val updateSQL = dialect.querySQL(
                    JDBCUpdate(table.name,
                               updateColumns.map(columnExpression),
                               whereClauses.map(_._1)))
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
        st => S.mapS(st)(t => deleteWriteOp(table.toKey(table.allColumns.iso.to(t))))

      private def deleteWriteOp(k: table.KeyList): JDBCWriteOp = {
        val whereClauses = colsOp(BinOp.EQ, table.keyColumns).apply(k)
        val deleteSQL    = dialect.querySQL(JDBCDelete(table.name, whereClauses.map(_._1)))
        JDBCWriteOp(deleteSQL, bindParameters(whereClauses.map(_._2)))
      }

    }

  def selectFrom(table: JDBCTable[C]) =
    new QueryBuilder[table.DataRec, HNil, HNil, HNil](
      table,
      dialect,
      effect,
      ColumnRecord.empty,
      identity
    )

  def deleteFrom(table: JDBCTable[C]) =
    new DeleteBuilder[table.DataRec, HNil](effect.S,
                                           table,
                                           dialect,
                                           _ => S.emit((Seq.empty, Seq.empty)))

  def query(table: JDBCTable[C]) =
    new QueryBuilder[table.DataRec, HNil, table.DataRec, table.Data](
      table,
      dialect,
      effect,
      toProjection(table.allColumns, ColumnReference),
      table.allColumns.iso.from
    )

  def byPK[WLen <: Nat](table: JDBCTable[C])(
      implicit length: Length.Aux[table.KeyList, WLen],
      split: Split.Aux[table.KeyList, WLen, table.KeyList, HNil])
    : QueryBuilder[table.DataRec, table.KeyList, table.DataRec, table.Data] = {
    implicit val kn = table.keySubset
    query(table).where(table.keyNames, BinOp.EQ)
  }

  def allRows(table: JDBCTable[C]): S[table.Data] =
    query(table).build[Unit].apply()

  def sqlRecord[In, Out]: PartialSQLRecord[In, Out] =
    new PartialSQLRecord[In, Out]

  class PartialSQLRecord[In, Out] {
    def apply[InR <: HList, OutR <: HList](sql: String)(
      implicit
      genIn: Generic.Aux[In, InR],
      genOut: Generic.Aux[Out, OutR],
      recIn: ColumnRecord[C, Unit, InR],
      recOut: ColumnRecord[C, Unit, OutR]): In => S[Out] = in => apply(sql, recIn.mapRecord(genIn.to(in), BindValues))

    def apply[OutR <: HList](sql: String, values: Seq[BoundValue])(
      implicit
      genOut: Generic.Aux[Out, OutR],
      recOut: ColumnRecord[C, Unit, OutR]): S[Out] = S.evalMap(effect.executeResultSet(sql, bindParameters(values))) { rs =>
      M.map(effect.resultSetRecord(recOut, 1, rs))(genOut.from)
    }
  }

  def executeSqlRecord[Out]: PartialSQLRecord[Nothing, Out] = new PartialSQLRecord

  def sql(sql: String): JDBCWriteOp = {
    JDBCWriteOp(sql, (con, ps) => Seq.empty[Any])
  }

  val ddl  = new {
    private def dialectSql(t: JDBCTable[C], d: SQLDialect => TableDefinition => String): JDBCWriteOp = {
      sql(d(dialect)(t.definition))
    }
    def dropAndCreate(t: JDBCTable[C]): S[JDBCWriteOp] = S.append(drop(t), create(t))

    def drop(t: JDBCTable[C]): S[JDBCWriteOp] =
      S.emit(dialectSql(t, _.dropTable))

    def create(t: JDBCTable[C]): S[JDBCWriteOp] = {
      S.emit(dialectSql(t, _.createTable))
    }
  }

  case class QueryBuilder[DataRec <: HList, InRec <: HList, OutRec <: HList, Out](
      private[jdbc] val table: TableRecord[C, DataRec],
      private[jdbc] val dialect: SQLDialect,
      private[jdbc] val E: JDBCEffect[S, F],
      private[jdbc] val projections: ColumnRecord[C, SQLProjection, OutRec],
      private[jdbc] val mapOut: OutRec => Out,
      private[jdbc] val toWhere: InRec => S[(Seq[JDBCWhereClause], Seq[BoundValue])] =
        (_: InRec) => S.emit((Seq.empty[JDBCWhereClause], Seq.empty[BoundValue])),
      private[jdbc] val orderCols: Seq[(NamedColumn, Boolean)] = Seq.empty,
      private[jdbc] val groupCols: Seq[NamedColumn] = Seq.empty
  ) extends WhereBuilder[DataRec, InRec] {
    type WhereOut[NewIn <: HList] = QueryBuilder[DataRec, NewIn, OutRec, Out]

    def count(
        implicit intCol: C[Int]
    ): QueryBuilder[DataRec, InRec, Int :: OutRec, Int :: OutRec] = {
      val newProjection = ColumnRecord.prepend(
        ColumnRecord[C, SQLProjection, Int :: HNil](
          Seq(SQLProjection(intCol.columnType, Aggregate(AggregateOp.Count, None)) -> intCol)
        ),
        projections
      )
      copy[DataRec, InRec, Int :: OutRec, Int :: OutRec](
        projections = newProjection,
        mapOut = identity
      )
    }

    def aggregate[T <: Symbol, CT <: HList, NewOutRec <: HList](w: Witness, op: AggregateOp.Value)(
        implicit c: ColumnSubsetBuilder.Aux[DataRec, w.T :: HNil, CT],
        prepend: Prepend.Aux[OutRec, CT, NewOutRec]
    ): QueryBuilder[DataRec, InRec, NewOutRec, NewOutRec] = {
      val newProjection =
        ColumnRecord.prepend(projections,
                             toProjection(table.allColumns.subset(c), n => Aggregate(op, Some(n))))
      copy[DataRec, InRec, prepend.Out, prepend.Out](projections = newProjection, mapOut = identity)
    }

    def cols[CT <: HList, NewOutRec <: HList, ColNames <: HList](cols: Cols[ColNames])(
        implicit c: ColumnSubsetBuilder.Aux[DataRec, ColNames, CT],
        prepend: Prepend.Aux[OutRec, CT, NewOutRec]
    ): QueryBuilder[DataRec, InRec, NewOutRec, NewOutRec] = {
      val newProjection =
        ColumnRecord.prepend(projections, toProjection(table.allColumns.subset(c), ColumnReference))
      copy[DataRec, InRec, prepend.Out, prepend.Out](projections = newProjection, mapOut = identity)
    }

    def groupBy[CT <: HList, ColNames <: HList](cols: Cols[ColNames])(
        implicit c: ColumnSubsetBuilder.Aux[DataRec, ColNames, CT]
    ): QueryBuilder[DataRec, InRec, OutRec, Out] =
      copy(
        groupCols =
          table.allColumns.subset(c).columns.map(cn => NamedColumn(cn._1, cn._2.columnType)))

    def orderBy[T <: Symbol](w: Witness, asc: Boolean)(
        implicit
        k: Keys.Aux[FieldType[w.T, Boolean] :: HNil, w.T :: HNil],
        toMap: ToMap.Aux[FieldType[w.T, Boolean] :: HNil, T, Boolean],
        sel: ColumnSubsetBuilder[DataRec, w.T :: HNil]
    ): QueryBuilder[DataRec, InRec, OutRec, Out] = orderWith(field[w.T](asc) :: HNil)

    def orderWith[OR <: HList, ORK <: HList, Syms <: Symbol](or: OR)(
        implicit keys: Keys.Aux[OR, ORK],
        toMap: ToMap.Aux[OR, Syms, Boolean],
        cssb: ColumnSubsetBuilder[DataRec, ORK]
    ): QueryBuilder[DataRec, InRec, OutRec, Out] = {
      val m         = toMap(or).map { case (s, b) => (s.name, b) }
      val actColMap = table.allColumns.columns.toMap
      val cols      = cssb.apply()._1.map(cn => (NamedColumn(cn, actColMap(cn).columnType), m(cn)))
      copy(orderCols = cols)
    }

    def buildAs[In, Out2](
        implicit c: AutoConvert[In, InRec],
        cout: AutoConvert[Out, Out2]
    ): In => S[Out2] = build[In].andThen(o => S.mapS(o)(cout.apply))

    def build[In](
        implicit c: AutoConvert[In, InRec],
    ): In => S[Out] = { in: In =>
      S.flatMapS(sqlStream[In].apply(in)) {
        case (selSQL, binds) =>
          S.mapS(E.streamForQuery(selSQL, bindParameters(binds), projections))(mapOut)
      }
    }

    def sqlStream[In](
        implicit c: AutoConvert[In, InRec]
    ): In => S[(String, Seq[BoundValue])] = in => {
      val baseSel =
        JDBCSelect(table.name,
                   projections.columns.map(_._1),
                   Seq.empty,
                   groupCols,
                   orderCols,
                   limit = false)
      S.mapS(toWhere(c(in))) {
        case (whereClauses, binds) =>
          (dialect.querySQL(baseSel.copy(where = whereClauses)), binds)
      }
    }

    def buildLimit[In](limit: Int)(
        implicit c: AutoConvert[In, InRec],
        intCol: C[Int]
    ): In => S[Out] = {
      val baseSel =
        JDBCSelect(table.name, projections.columns.map(_._1), Seq.empty, groupCols, orderCols, true)
      w2: In =>
        S.flatMapS(toWhere(c(w2))) {
          case (whereClauses, binds) =>
            val selSQL = dialect.querySQL(baseSel.copy(where = whereClauses))
            S.mapS(
              E.streamForQuery(selSQL,
                               bindParameters(binds ++ Seq(intCol.bindValue(limit))),
                               projections))(mapOut)
        }
    }

    override def withToWhere[NewIn <: HList](
        f: NewIn => S[(Seq[JDBCWhereClause], Seq[BoundValue])])
      : QueryBuilder[DataRec, NewIn, OutRec, Out] =
      copy(toWhere = f)
  }

  case class DeleteBuilder[DataRec <: HList, InRec <: HList](
      private[jdbc] val S: Streamable[S, F],
      private[jdbc] val table: TableRecord[C, DataRec],
      private[jdbc] val dialect: SQLDialect,
      private[jdbc] val toWhere: InRec => S[(Seq[JDBCWhereClause], Seq[BoundValue])]
  ) extends WhereBuilder[DataRec, InRec] {
    type WhereOut[NewIn <: HList] = DeleteBuilder[DataRec, NewIn]

    def build[W2](
        implicit
        c: AutoConvert[W2, InRec]): W2 => S[JDBCWriteOp] = w => {
      S.mapS(toWhere(c(w))) {
        case (where, values) =>
          val deleteSQL = dialect.querySQL(JDBCDelete(table.name, where))
          JDBCWriteOp(deleteSQL, bindParameters(values))
      }
    }

    override def withToWhere[NewIn <: HList](
        f: NewIn => S[(Seq[JDBCWhereClause], Seq[BoundValue])])
      : DeleteBuilder[DataRec, NewIn] =
      copy(toWhere = f)
  }

  trait WhereBuilder[DataRec <: HList, InRec <: HList] {
    type WhereOut[NewIn <: HList]

    private[jdbc] def table: TableRecord[C, DataRec]

    private[jdbc] def toWhere: InRec => S[(Seq[JDBCWhereClause], Seq[BoundValue])]

    protected def withToWhere[NewIn <: HList](
        f: NewIn => S[(Seq[JDBCWhereClause], Seq[BoundValue])]): WhereOut[NewIn]

    protected def addWhere[NewIn <: HList](
        f: NewIn => (InRec, Seq[JDBCWhereClause], Seq[BoundValue])): WhereOut[NewIn] = withToWhere {
      newIn =>
        val (oldIn, newClause, newBind) = f(newIn)
        S.mapS(this.toWhere(oldIn)) {
          case (oldClause, oldBind) => (oldClause ++ newClause, oldBind ++ newBind)
        }
    }

    def whereInNotEmpty[A, NewIn <: HList, WLen <: Nat](whereCol: Witness)(
        implicit
        ev: whereCol.T <:< Symbol,
        select: Selector.Aux[DataRec, whereCol.T, A],
        prepend: Prepend.Aux[InRec, NonEmptyList[A] :: HNil, NewIn],
        length: Length.Aux[InRec, WLen],
        split: Split.Aux[NewIn, WLen, InRec, NonEmptyList[A] :: HNil]
    ): WhereOut[NewIn] = {
      val (colName, inCol) = table.allColumns.singleColumn(whereCol)
      addWhere { newIn =>
        val (oldWhere, newWhere)  = split(newIn)
        val (newClause, bindVals) = mkInClause(colName, inCol, newWhere.head.toList)
        (oldWhere, newClause, bindVals)
      }
    }

    private def mkInClause[A](colName: String, inCol: C[A], vals: Seq[A]) = {
      val bindVals = vals.map(inCol.bindValue)
      val clause = BinClause(ColumnReference(NamedColumn(colName, inCol.columnType)),
                             BinOp.IN,
                             Expressions(bindVals.map(_ => Parameter(inCol.columnType))))
      (Seq(clause), bindVals)
    }

    def whereIn[A, NewIn <: HList, WLen <: Nat](whereCol: Witness)(
        implicit
        select: Selector.Aux[DataRec, whereCol.T, A],
        ev: whereCol.T <:< Symbol,
        prepend: Prepend.Aux[InRec, S[A] :: HNil, NewIn],
        length: Length.Aux[InRec, WLen],
        split: Split.Aux[NewIn, WLen, InRec, S[A] :: HNil]
    ): WhereOut[NewIn] = {
      val (colName, inCol) = table.allColumns.singleColumn(whereCol)
      withToWhere { newIn =>
        val (oldWhere, newWhere) = split(newIn)
        S.flatMapS(toWhere(oldWhere)) {
          case (oldClause, oldBind) =>
            S.maxMapped(dialect.maxInParamaters, newWhere.head) { vals =>
              val (newClause, newBind) = mkInClause(colName, inCol, vals)
              (oldClause ++ newClause, oldBind ++ newBind)
            }
        }
      }
    }

    def where[WhereVals <: HList, NewIn <: HList, WLen <: Nat](whereCol: Witness, binOp: BinOp)(
        implicit
        csb: ColumnSubsetBuilder.Aux[DataRec, whereCol.T :: HNil, WhereVals],
        prepend: Prepend.Aux[InRec, WhereVals, NewIn],
        length: Length.Aux[InRec, WLen],
        split: Split.Aux[NewIn, WLen, InRec, WhereVals]
    ): WhereOut[NewIn] = where(Cols(whereCol), binOp)

    def where[ColNames <: HList, WhereVals <: HList, NewIn <: HList, WLen <: Nat](
        whereCols: Cols[ColNames],
        binOp: BinOp)(
        implicit
        css: ColumnSubsetBuilder.Aux[DataRec, ColNames, WhereVals],
        len: Length.Aux[InRec, WLen],
        prepend: Prepend.Aux[InRec, WhereVals, NewIn],
        split: Split.Aux[NewIn, WLen, InRec, WhereVals]
    ): WhereOut[NewIn] = {
      val whereCols = table.allColumns.subset(css)
      addWhere { newIn =>
        val (oldWhere, newWhere) = split(newIn)
        val opWheres             = colsOp(binOp, whereCols).apply(newWhere)
        (oldWhere, opWheres.map(_._1), opWheres.map(_._2))
      }
    }
  }

}

object JDBCQueries {

  def colsOp[C[_] <: JDBCColumn[_], R <: HList, K <: HList](
      op: BinOp,
      where: ColumnSubset[C, R, K]
  ): K => Seq[(JDBCWhereClause, BoundValue)] = k => {
    where.mapRecord(k, BindNamedValues).map { bv =>
      (BinClause(ColumnReference(bv.namedColumn), op, Parameter(bv.column.columnType)), bv.binder)
    }
  }

  def bindParameters(params: Seq[BoundValue]): (Connection, PreparedStatement) => Seq[Any] =
    (con, ps) => {
      @tailrec
      def loop(i: Int, offs: Int, outvals: mutable.Buffer[Any]): mutable.Buffer[Any] = {
        if (i < params.length) {
          val c        = params(i)
          val nextOffs = c.bind(offs, con, ps)
          loop(i + 1, nextOffs, outvals += c.value)
        } else outvals
      }

      loop(0, 1, mutable.Buffer.empty)
    }

  def columnExpression[A](bound: BoundColumn): ColumnExpression =
    ColumnExpression(bound.namedColumn, Parameter(bound.column.columnType))

  def toProjection[C[_] <: JDBCColumn[_], R <: HList](
      cols: ColumnRecord[C, String, R],
      f: NamedColumn => SQLExpression
  ): ColumnRecord[C, SQLProjection, R] = {
    ColumnRecord {
      cols.columns.map {
        case (name, col) =>
          (SQLProjection(col.columnType, f(NamedColumn(name, col.columnType))), col)
      }
    }
  }
}
