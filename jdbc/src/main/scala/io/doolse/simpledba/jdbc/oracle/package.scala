package io.doolse.simpledba.jdbc

import java.time.Instant

import cats.effect.implicits._
import fs2.Stream
import io.doolse.simpledba.jdbc.StandardJDBC._
import io.doolse.simpledba.{AutoConvert, ColumnSubsetBuilder, Columns, Iso}
import shapeless.ops.hlist.RemoveAll
import shapeless.ops.record.Keys
import shapeless.{::, HList, HNil}

package object oracle {

  case class OracleColumn[AA](wrapped: StdJDBCColumn[AA], columnType: ColumnType) extends WrappedColumn[AA]

  trait StdOracleColumns extends StdColumns {
    type C[A] = OracleColumn[A]

    implicit def stringCol = OracleColumn(StdJDBCColumn.stringCol, ColumnType("NCLOB"))

    implicit def longCol = OracleColumn[Long](StdJDBCColumn.longCol, ColumnType("NUMBER(19)"))

    implicit def intCol = OracleColumn[Int](StdJDBCColumn.intCol, ColumnType("INTEGER"))

    implicit def boolCol = OracleColumn[Boolean](StdJDBCColumn.boolCol, ColumnType("NUMBER(1,0)"))

    implicit def instantCol = OracleColumn[Instant](StdJDBCColumn.instantCol, ColumnType("TIMESTAMP"))

    implicit def floatCol = OracleColumn[Float](StdJDBCColumn.floatCol, ColumnType(???))

    implicit def doubleCol = OracleColumn[Double](StdJDBCColumn.doubleCol, ColumnType(???))

    override def wrap[A, B](col: OracleColumn[A], edit: StdJDBCColumn[A] => StdJDBCColumn[B],
                            editType: ColumnType => ColumnType): OracleColumn[B] =
      col.copy(wrapped = edit(col.wrapped), columnType = editType(col.columnType))

    override def sizedStringType(size: Int): String = s"NVARCHAR2($size)"
  }

  object OracleColumn extends StdOracleColumns

  def oracleTypes(stringKeySize: Int)(b: ColumnType, key: Boolean): String = {
    if (key && b.typeName == "NCLOB") s"NVARCHAR2($stringKeySize)" else b.typeName
  }


  val oracleReserved = DefaultReserved ++ Set("session", "timestamp")

  val oracleEscapeReserved = escapeReserved(oracleReserved) _

  def oracleSQLExpr(config: JDBCConfig)(sql: SQLExpression): String = {
    sql match {
      case FunctionCall("nextval", Seq(SQLString(named))) => s"$named.nextval"
      case o => stdExpressionSQL(config)(o)
    }
  }

  val oracleConfig = JDBCSQLConfig[OracleColumn](oracleEscapeReserved, oracleEscapeReserved, stdSQLQueries,
    oracleSQLExpr, oracleTypes(256),
    OracleSchemaSQL.apply)

  case class OracleSchemaSQL(config: JDBCConfig) extends StandardSchemaSQL(config) {
    override def dropTable(t: TableDefinition): String =
      s"BEGIN EXECUTE IMMEDIATE 'DROP TABLE ${config.escapeTableName(t.name)}'; EXCEPTION WHEN OTHERS THEN NULL; END;"

    override def addColumns(t: TableColumns): Seq[String] = {
      def mkAddCol(cb: NamedColumn) = s"${col(cb)} ${config.typeName(cb.columnType, false)}"

      Seq(s"ALTER TABLE ${config.escapeTableName(t.name)} ADD ${t.columns.map(mkAddCol).mkString("(", ",", ")")}")
    }
  }

  def insertWith[A, C[_] <: JDBCColumn, T, R <: HList,
  KeyNames <: HList,
  AllCols <: HList,
  WithoutKeys <: HList,
  JustKeys <: HList,
  Res](table: JDBCTable.Aux[C, T, R, A :: HNil, KeyNames], sequence: Sequence[A])(
    implicit keys: Keys.Aux[R, AllCols], removeAll: RemoveAll.Aux[AllCols, KeyNames, (WithoutKeys, JustKeys)],
    withoutKeys: ColumnSubsetBuilder[R, JustKeys],
    sampleValue: SampleValue[A],
    conv: AutoConvert[Res, Stream[JDBCIO, A => T]]): Res => Stream[JDBCIO, T] = {
    res =>
      conv(res).flatMap { f =>
        val fullRec = table.allColumns.iso.to(f(sampleValue.v))
        val sscols = table.allColumns.subset(withoutKeys)
        val keyCols = table.keyColumns.columns
        val seqExpr = FunctionCall("nextval", Seq(SQLString(sequence.name)))
        val colValues = JDBCQueries.bindValues(sscols._1, sscols._2(fullRec)).columns
        val colBindings = Seq(keyCols.head._1 -> seqExpr) ++ colValues.map {
          case ((_, name), col) => name -> Parameter(col.columnType)
        }
        val mc = table.config

        val insertSQL =
          s"INSERT INTO ${mc.escapeTableName(table.name)} " +
            s"${brackets(colBindings.map(v => mc.escapeColumnName(v._1)))} " +
            s"VALUES ${
              brackets(colBindings.map(v =>
                mc.exprToSQL(v._2)))
            }"

        val insert = JDBCRawSQL(insertSQL)
        JDBCQueries.prepareAndQuery(mc, insert,
          JDBCQueries.bindParameters(colValues.map(_._1._1)).map(c => Seq(ValueLog(c))),
          (con,sql) => con.prepareStatement(sql, keyCols.map(_._1).toArray),
          { ps => ps.executeUpdate(); ps.getGeneratedKeys() }
        ).evalMap { rs =>
          JDBCQueries.getColRecord(Columns(keyCols, Iso.id[A :: HNil]), 1, rs).liftIO[JDBCIO]
        }.map { a => f(a.head) }
      }
  }
}
