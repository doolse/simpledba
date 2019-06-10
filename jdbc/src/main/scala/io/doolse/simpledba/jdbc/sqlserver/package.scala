package io.doolse.simpledba.jdbc

import java.time.Instant

import io.doolse.simpledba._
import shapeless.ops.hlist.RemoveAll
import shapeless.ops.record.Keys
import shapeless.{::, HList, HNil}

package object sqlserver {

  case class SQLServerColumn[A](wrapped: StdJDBCColumn[A], columnType: ColumnType)
      extends WrappedColumn[A]

  trait StdSQLServerColumns extends StdColumns[SQLServerColumn] {

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

  trait SQLServerDialect extends StdSQLDialect {
    val SqlServerReserved = DefaultReserved ++ Set("key")

    override def reservedIdentifiers: Set[String] = SqlServerReserved

    def stringKeySize: Int

    override def typeName(b: ColumnType, key: Boolean): String = {
      val baseType =
        if (key && b.typeName == "NVARCHAR(MAX)") s"NVARCHAR($stringKeySize)" else b.typeName
      if (b.hasFlag(IdentityColumn)) {
        s"$baseType IDENTITY"
      } else baseType
    }

    override def dropTable(t: TableDefinition): String =
      s"DROP TABLE IF EXISTS ${escapeTableName(t.name)}"

    override def addColumns(t: TableColumns): Seq[String] = {
      def mkAddCol(cb: NamedColumn) = s"${col(cb)} ${typeName(cb.columnType, false)}"

      Seq(
        s"ALTER TABLE ${escapeTableName(t.name)} ADD ${t.columns.map(mkAddCol).mkString(",")}"
      )
    }

  }

  object SQLServerDialect extends SQLServerDialect {
    override def stringKeySize: Int = 256
  }

  val sqlServerMapper = JDBCMapper[SQLServerColumn](SQLServerDialect)

  case object IdentityColumn

  def identityCol[A](implicit c: SQLServerColumn[A]): SQLServerColumn[A] = {
    c.copy(columnType = c.columnType.withFlag(IdentityColumn))
  }

  class SQLServerQueries[S[_], F[_]](dialect: SQLDialect, E: JDBCEffect[S, F]) {
    def insertIdentity[T,
                       R <: HList,
                       KeyNames <: HList,
                       AllCols <: HList,
                       WithoutKeys <: HList,
                       JustKeys <: HList,
                       A,
                       Res](
        table: JDBCTable.Aux[SQLServerColumn, T, R, A :: HNil, KeyNames]
    )(
        implicit keys: Keys.Aux[R, AllCols],
        removeAll: RemoveAll.Aux[AllCols, KeyNames, (WithoutKeys, JustKeys)],
        withoutKeys: ColumnSubsetBuilder[R, JustKeys],
        sampleValue: SampleValue[A],
        conv: AutoConvert[Res, S[A => T]]
    ): Res => S[T] = { res =>
      val S  = E.S
      val SM = S.SM
      SM.flatMap(conv(res)) { f =>
        val fullRec   = table.allColumns.iso.to(f(sampleValue.v))
        val sscols    = table.allColumns.subset(withoutKeys)
        val keyCols   = table.keyColumns.columns
        val colValues = sscols.mapRecord(sscols.from(fullRec), BindValues)
        val colBindings = colValues.map { bc =>
          bc.name -> Parameter(bc.column.columnType)
        }
        import StdSQLDialect._
        import dialect._
        val insertSQL =
          s"INSERT INTO ${escapeTableName(table.name)} " +
            s"${brackets(colBindings.map(v => escapeColumnName(v._1)))} " +
            s"OUTPUT ${keyCols.map(k => s"INSERTED.${escapeColumnName(k._1)}").mkString(",")} " +
            s"VALUES ${brackets(colBindings.map(v => expressionSQL(v._2)))}"

        SM.map(
          E.streamForQuery(
            insertSQL,
            JDBCQueries.bindParameters(colValues.map(_.binder)).map(c => Seq(ValueLog(c))),
            Columns(keyCols, Iso.id[A :: HNil])
          )) { a =>
          f(a.head)
        }
      }
    }
  }
}
