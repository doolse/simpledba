package io.doolse.simpledba.jdbc

import java.time.Instant

import fs2.Stream
import io.doolse.simpledba._
import io.doolse.simpledba.jdbc.StandardJDBC._
import shapeless.ops.hlist.RemoveAll
import shapeless.ops.record.Keys
import shapeless.{::, HList, HNil}

package object sqlserver {

  case class SQLServerColumn[A](wrapped: StdJDBCColumn[A], columnType: ColumnType)
      extends WrappedColumn[A]

  trait StdSQLServerColumns extends StdColumns {
    type C[A] = SQLServerColumn[A]

    implicit def longCol = SQLServerColumn[Long](StdJDBCColumn.longCol, ColumnType("BIGINT"))

    implicit def intCol = SQLServerColumn[Int](StdJDBCColumn.intCol, ColumnType("INTEGER"))

    implicit def stringCol =
      SQLServerColumn[String](StdJDBCColumn.stringCol, ColumnType("NVARCHAR(MAX)"))

    implicit def boolCol = SQLServerColumn[Boolean](StdJDBCColumn.boolCol, ColumnType("BIT"))

    implicit def instantCol =
      SQLServerColumn[Instant](StdJDBCColumn.instantCol, ColumnType("DATETIME"))

    implicit def floatCol = SQLServerColumn[Float](StdJDBCColumn.floatCol, ColumnType(???))

    implicit def doubleCol = SQLServerColumn[Double](StdJDBCColumn.doubleCol, ColumnType(???))

    override def wrap[A, B](
        col: SQLServerColumn[A],
        edit: StdJDBCColumn[A] => StdJDBCColumn[B],
        editType: ColumnType => ColumnType
    ): SQLServerColumn[B] =
      col.copy(wrapped = edit(col.wrapped), columnType = editType(col.columnType))

    override def sizedStringType(size: Int): String = s"NVARCHAR($size)"
  }

  object SQLServerColumn extends StdSQLServerColumns

  def sqlServerTypeNames(stringKeySize: Int)(b: ColumnType, key: Boolean): String = {
    val baseType =
      if (key && b.typeName == "NVARCHAR(MAX)") s"NVARCHAR($stringKeySize)" else b.typeName
    if (b.hasFlag(IdentityColumn)) {
      s"$baseType IDENTITY"
    } else baseType
  }

  def sqlServerConfig =
    JDBCSQLConfig[SQLServerColumn](
      defaultEscapeReserved,
      defaultEscapeReserved,
      stdSQLQueries,
      stdExpressionSQL,
      sqlServerTypeNames(256),
      SQLServerSQL.apply
    )

  case class SQLServerSQL(config: JDBCConfig) extends StandardSchemaSQL(config) {
    override def dropTable(t: TableDefinition): String =
      s"DROP TABLE IF EXISTS ${config.escapeTableName(t.name)}"

    override def addColumns(t: TableColumns): Seq[String] = {
      def mkAddCol(cb: NamedColumn) = s"${col(cb)} ${config.typeName(cb.columnType, false)}"

      Seq(
        s"ALTER TABLE ${config.escapeTableName(t.name)} ADD ${t.columns.map(mkAddCol).mkString(",")}"
      )
    }
  }

  case object IdentityColumn

  def identityCol[A](implicit c: SQLServerColumn[A]): SQLServerColumn[A] = {
    c.copy(columnType = c.columnType.withFlag(IdentityColumn))
  }

  def insertIdentity[C[_] <: JDBCColumn,
                     T,
                     R <: HList,
                     KeyNames <: HList,
                     AllCols <: HList,
                     WithoutKeys <: HList,
                     JustKeys <: HList,
                     A,
                     Res](
      table: JDBCTable.Aux[C, T, R, A :: HNil, KeyNames]
  )(
      implicit keys: Keys.Aux[R, AllCols],
      removeAll: RemoveAll.Aux[AllCols, KeyNames, (WithoutKeys, JustKeys)],
      withoutKeys: ColumnSubsetBuilder[R, JustKeys],
      sampleValue: SampleValue[A],
      conv: AutoConvert[Res, Stream[JDBCIO, A => T]]
  ): Res => Stream[JDBCIO, T] = { res =>
    conv(res).flatMap { f =>
      val fullRec   = table.allColumns.iso.to(f(sampleValue.v))
      val sscols    = table.allColumns.subset(withoutKeys)
      val keyCols   = table.keyColumns.columns
      val colValues = JDBCQueries.bindValues(sscols._1, sscols._2(fullRec)).columns
      val colBindings = colValues.map {
        case ((_, name), col) => name -> Parameter(col.columnType)
      }
      val mc = table.config
      val insertSQL =
        s"INSERT INTO ${mc.escapeTableName(table.name)} " +
          s"${brackets(colBindings.map(v => mc.escapeColumnName(v._1)))} " +
          s"OUTPUT ${keyCols.map(k => s"INSERTED.${mc.escapeColumnName(k._1)}").mkString(",")} " +
          s"VALUES ${brackets(colBindings.map(v => mc.exprToSQL(v._2)))}"

      val insert = JDBCRawSQL(insertSQL)
      JDBCQueries
        .streamForQuery(
          table.config,
          insert,
          JDBCQueries.bindParameters(colValues.map(_._1._1)).map(c => Seq(ValueLog(c))),
          Columns(keyCols, Iso.id[A :: HNil])
        )
        .map { a =>
          f(a.head)
        }
    }
  }
}
