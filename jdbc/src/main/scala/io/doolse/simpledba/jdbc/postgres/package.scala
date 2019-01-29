package io.doolse.simpledba.jdbc

import java.sql.JDBCType
import java.time.Instant
import java.util.UUID

import fs2.Stream
import io.doolse.simpledba._
import io.doolse.simpledba.jdbc.StandardJDBC._
import shapeless._
import shapeless.ops.hlist.RemoveAll
import shapeless.ops.record.Keys

import scala.reflect.ClassTag

package object postgres {

  case class PostgresColumn[A](wrapped: StdJDBCColumn[A], columnType: ColumnType) extends WrappedColumn[A]

  trait StdPostgresColumns extends StdColumns
  {
    type C[A] = PostgresColumn[A]

    implicit def uuidCol = PostgresColumn[UUID](StdJDBCColumn.uuidCol(JDBCType.NULL), ColumnType("UUID"))

    implicit def longCol = PostgresColumn[Long](StdJDBCColumn.longCol, ColumnType("BIGINT"))

    implicit def intCol = PostgresColumn[Int](StdJDBCColumn.intCol, ColumnType("INTEGER"))

    implicit def stringCol = PostgresColumn[String](StdJDBCColumn.stringCol, ColumnType("TEXT"))

    implicit def boolCol = PostgresColumn[Boolean](StdJDBCColumn.boolCol, ColumnType("BOOLEAN"))

    implicit def instantCol = PostgresColumn[Instant](StdJDBCColumn.instantCol, ColumnType("TIMESTAMP"))

    implicit def floatCol = PostgresColumn[Float](StdJDBCColumn.floatCol, ColumnType("REAL"))

    implicit def doubleCol = PostgresColumn[Double](StdJDBCColumn.doubleCol, ColumnType("DOUBLE PRECISION"))

    implicit def pgArrayCol[A : ClassTag](implicit inner: PostgresColumn[A]): PostgresColumn[Array[A]] =
      PostgresColumn(StdJDBCColumn.arrayCol(inner.columnType.typeName, inner.wrapped),
        inner.columnType.copy(typeName = inner.columnType.typeName + "[]"))

    override def wrap[A, B](col: PostgresColumn[A],
                            edit: StdJDBCColumn[A] => StdJDBCColumn[B],
                            editType: ColumnType => ColumnType): PostgresColumn[B] =
      col.copy(wrapped = edit(col.wrapped), columnType = editType(col.columnType))

    override def sizedStringType(size: Int): String = s"VARCHAR($size)"
  }

  object PostgresColumn extends StdPostgresColumns

  def postgresExpressions(config: JDBCConfig)(c: SQLExpression): String = c match {
    case Parameter(ColumnType("JSONB", _, _)) => "?::JSONB"
    case o => stdExpressionSQL(config)(o)
  }

  val postgresConfig = JDBCSQLConfig[PostgresColumn](defaultEscapeReserved, defaultEscapeReserved,
    stdSQLQueries, postgresExpressions, stdTypeNames, PgSchemaSQL.apply)

  case class PgSchemaSQL(config: JDBCConfig) extends StandardSchemaSQL(config) {
    override def dropTable(t: TableDefinition): String =
      s"DROP TABLE IF EXISTS ${config.escapeTableName(t.name)} CASCADE"
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
            } RETURNING ${keyCols.map(k => mc.escapeColumnName(k._1)).mkString(",")}"

        val insert = JDBCRawSQL(insertSQL)
        JDBCQueries.streamForQuery(table.config, insert,
          JDBCQueries.bindParameters(colValues.map(_._1._1)).map(c => Seq(ValueLog(c))), Columns(keyCols, Iso.id[A :: HNil])).map {
          a => f(a.head)
        }
      }
  }
}
