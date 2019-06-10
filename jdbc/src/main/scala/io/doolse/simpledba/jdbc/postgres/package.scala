package io.doolse.simpledba.jdbc

import java.sql.JDBCType
import java.time.Instant
import java.util.UUID

import io.doolse.simpledba._
import shapeless._
import shapeless.ops.hlist.RemoveAll
import shapeless.ops.record.Keys

import scala.reflect.ClassTag

package object postgres {

  case class PostgresColumn[A](wrapped: StdJDBCColumn[A], columnType: ColumnType)
      extends WrappedColumn[A]

  trait StdPostgresColumns extends StdColumns[PostgresColumn] {
    implicit def uuidCol =
      PostgresColumn[UUID](StdJDBCColumn.uuidCol(JDBCType.NULL), ColumnType("UUID"))

    implicit def longCol = PostgresColumn[Long](StdJDBCColumn.longCol, ColumnType("BIGINT"))

    implicit def intCol = PostgresColumn[Int](StdJDBCColumn.intCol, ColumnType("INTEGER"))

    implicit def stringCol = PostgresColumn[String](StdJDBCColumn.stringCol, ColumnType("TEXT"))

    implicit def boolCol = PostgresColumn[Boolean](StdJDBCColumn.boolCol, ColumnType("BOOLEAN"))

    implicit def instantCol =
      PostgresColumn[Instant](StdJDBCColumn.instantCol, ColumnType("TIMESTAMP"))

    implicit def floatCol = PostgresColumn[Float](StdJDBCColumn.floatCol, ColumnType("REAL"))

    implicit def doubleCol =
      PostgresColumn[Double](StdJDBCColumn.doubleCol, ColumnType("DOUBLE PRECISION"))

    implicit def pgArrayCol[A: ClassTag](
        implicit inner: PostgresColumn[A]
    ): PostgresColumn[Array[A]] =
      PostgresColumn(
        StdJDBCColumn.arrayCol(inner.columnType.typeName, inner.wrapped),
        inner.columnType.copy(typeName = inner.columnType.typeName + "[]")
      )

    override def wrap[A, B](
        col: PostgresColumn[A],
        edit: StdJDBCColumn[A] => StdJDBCColumn[B],
        editType: ColumnType => ColumnType
    ): PostgresColumn[B] =
      col.copy(wrapped = edit(col.wrapped), columnType = editType(col.columnType))

    override def sizedStringType(size: Int): String = s"VARCHAR($size)"
  }

  object PostgresColumn extends StdPostgresColumns

  trait PostgresDialect extends StdSQLDialect {
    override def expressionSQL(expression: SQLExpression): String = expression match {
      case Parameter(ColumnType("JSONB", _, _)) => "?::JSONB"
      case o                                    => stdExpressionSQL(o)
    }
    override def dropTable(t: TableDefinition): String =
      s"DROP TABLE IF EXISTS ${escapeTableName(t.name)} CASCADE"
  }

  object PostgresDialect extends PostgresDialect

  val postgresMapper = JDBCMapper[PostgresColumn](PostgresDialect)

  class PostgresQueries[S[_], F[_]](dialect: SQLDialect, E: JDBCEffect[S, F]) {
    def insertWith[A,
                   T,
                   R <: HList,
                   KeyNames <: HList,
                   AllCols <: HList,
                   WithoutKeys <: HList,
                   JustKeys <: HList,
                   Res](
        table: JDBCTable.Aux[PostgresColumn, T, R, A :: HNil, KeyNames],
        sequence: Sequence[A]
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
        val seqExpr   = FunctionCall("nextval", Seq(SQLString(sequence.name)))
        val colValues = sscols.mapRecord(sscols.from(fullRec), BindValues)
        val colBindings = Seq(keyCols.head._1 -> seqExpr) ++ colValues.map { bc =>
          bc.name -> Parameter(bc.column.columnType)
        }
        import StdSQLDialect._
        import dialect._

        val insertSQL =
          s"INSERT INTO ${escapeTableName(table.name)} " +
            s"${brackets(colBindings.map(v => escapeColumnName(v._1)))} " +
            s"VALUES ${brackets(colBindings.map(v => expressionSQL(v._2)))} RETURNING ${keyCols
              .map(k => escapeColumnName(k._1))
              .mkString(",")}"

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
