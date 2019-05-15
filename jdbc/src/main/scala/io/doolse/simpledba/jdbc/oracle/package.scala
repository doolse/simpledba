package io.doolse.simpledba.jdbc

import java.sql.{PreparedStatement, ResultSet}
import java.time.Instant

import fs2.Stream
import io.doolse.simpledba.jdbc.hsql.{HSQLColumn, HSQLDialect}
import io.doolse.simpledba.{AutoConvert, ColumnSubsetBuilder, Columns, Iso}
import shapeless.ops.hlist.RemoveAll
import shapeless.ops.record.Keys
import shapeless.{::, HList, HNil}

package object oracle {

  case class OracleColumn[AA](wrapped: StdJDBCColumn[AA], columnType: ColumnType)
      extends WrappedColumn[AA]

  trait StdOracleColumns extends StdColumns[OracleColumn] {
    implicit def stringCol = OracleColumn(StdJDBCColumn.stringCol, ColumnType("NCLOB"))

    implicit def longCol = OracleColumn[Long](StdJDBCColumn.longCol, ColumnType("NUMBER(19)"))

    implicit def intCol = OracleColumn[Int](StdJDBCColumn.intCol, ColumnType("INTEGER"))

    implicit def boolCol = OracleColumn[Boolean](StdJDBCColumn.boolCol, ColumnType("NUMBER(1,0)"))

    implicit def instantCol =
      OracleColumn[Instant](StdJDBCColumn.instantCol, ColumnType("TIMESTAMP"))

    implicit def floatCol = OracleColumn[Float](StdJDBCColumn.floatCol, ColumnType(???))

    implicit def doubleCol = OracleColumn[Double](StdJDBCColumn.doubleCol, ColumnType(???))

    override def wrap[A, B](
        col: OracleColumn[A],
        edit: StdJDBCColumn[A] => StdJDBCColumn[B],
        editType: ColumnType => ColumnType
    ): OracleColumn[B] =
      col.copy(wrapped = edit(col.wrapped), columnType = editType(col.columnType))

    override def sizedStringType(size: Int): String = s"NVARCHAR2($size)"
  }

  object OracleColumn extends StdOracleColumns

  trait OracleDialect extends StdSQLDialect {
    val OracleReserved                            = DefaultReserved ++ Set("session", "timestamp", "key")
    override def reservedIdentifiers: Set[String] = OracleReserved

    override def expressionSQL(expr: SQLExpression): String = {
      expr match {
        case FunctionCall("nextval", Seq(SQLString(named))) => s"$named.nextval"
        case o                                              => stdExpressionSQL(o)
      }
    }

    def stringKeySize: Int

    override def typeName(c: ColumnType, keyColumn: Boolean): String = {
      if (keyColumn && c.typeName == "NCLOB") s"NVARCHAR2($stringKeySize)" else c.typeName
    }

    override def dropTable(t: TableDefinition): String =
      s"BEGIN EXECUTE IMMEDIATE 'DROP TABLE ${escapeTableName(t.name)}'; EXCEPTION WHEN OTHERS THEN NULL; END;"

    override def addColumns(t: TableColumns): Seq[String] = {
      def mkAddCol(cb: NamedColumn) = s"${col(cb)} ${typeName(cb.columnType, false)}"

      Seq(
        s"ALTER TABLE ${escapeTableName(t.name)} ADD ${t.columns.map(mkAddCol).mkString("(", ",", ")")}"
      )
    }

  }

  object OracleDialect extends OracleDialect {
    def stringKeySize = 256
  }

  val oracleMapper = JDBCMapper[OracleColumn](OracleDialect)

  case class OracleQueries[F[_]](dialect: SQLDialect, E: JDBCEffect[F]) {

    def insertWith[A,
                   T,
                   R <: HList,
                   KeyNames <: HList,
                   AllCols <: HList,
                   WithoutKeys <: HList,
                   JustKeys <: HList,
                   Res](
        table: JDBCTable.Aux[OracleColumn, T, R, A :: HNil, KeyNames],
        sequence: Sequence[A]
    )(
        implicit
        keys: Keys.Aux[R, AllCols],
        removeAll: RemoveAll.Aux[AllCols, KeyNames, (WithoutKeys, JustKeys)],
        withoutKeys: ColumnSubsetBuilder[R, JustKeys],
        sampleValue: SampleValue[A],
        conv: AutoConvert[Res, Stream[F, A => T]]
    ): Res => Stream[F, T] = { res =>
      conv(res).flatMap { f =>
        val fullRec   = table.allColumns.iso.to(f(sampleValue.v))
        val sscols    = table.allColumns.subset(withoutKeys)
        val keyCols   = table.keyColumns.columns
        val seqExpr   = FunctionCall("nextval", Seq(SQLString(sequence.name)))
        val colValues = JDBCQueries.bindValues(sscols._1, sscols._2(fullRec)).columns
        val colBindings = Seq(keyCols.head._1 -> seqExpr) ++ colValues.map {
          case ((_, name), col) => name -> Parameter(col.columnType)
        }
        val insertSQL =
          s"INSERT INTO ${dialect.escapeTableName(table.name)} " +
            s"${StdSQLDialect.brackets(colBindings.map(v => dialect.escapeColumnName(v._1)))} " +
            s"VALUES ${StdSQLDialect.brackets(colBindings.map(v => dialect.expressionSQL(v._2)))}"

        val binder =
          JDBCQueries.bindParameters(colValues.map(_._1._1)).map(c => Seq(ValueLog(c): BindLog))
        E.executeStream[PreparedStatement, ResultSet](
            E.logAndPrepare(insertSQL,
                            _.prepareStatement(insertSQL, keyCols.map(_._1).toArray[String])),
            E.logAndBind(insertSQL, binder, ps => { ps.executeUpdate; ps.getGeneratedKeys })
          )
          .evalMap(rs => E.resultSetRecord(Columns(keyCols, Iso.id[A :: HNil]), 1, rs))
          .map(a => f(a.head))
      }
    }
  }
}
