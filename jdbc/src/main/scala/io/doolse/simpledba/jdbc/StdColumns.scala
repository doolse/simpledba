package io.doolse.simpledba.jdbc

import java.time.Instant

import io.doolse.simpledba.Iso

trait StdColumns[C[_] <: JDBCColumn] {

  def wrap[A, B](
      col: C[A],
      edit: StdJDBCColumn[A] => StdJDBCColumn[B],
      editType: ColumnType => ColumnType
  ): C[B]
  def sizedStringType(size: Int): String

  implicit def stringCol: C[String]

  implicit def longCol: C[Long]

  implicit def intCol: C[Int]

  implicit def boolCol: C[Boolean]

  implicit def floatCol: C[Float]

  implicit def doubleCol: C[Double]

  implicit def instantCol: C[Instant]

  implicit def isoCol[A, B](implicit iso: Iso[B, A], col: C[A]): C[B] =
    wrap[A, B](col, _.isoMap(iso), identity)

  implicit def optionalCol[A](implicit col: C[A]): C[Option[A]] =
    wrap(col, StdJDBCColumn.optionalColumn, _.copy(nullable = true))

  implicit def sizedStringIso[A](implicit sizedIso: SizedIso[A, String], col: C[String]): C[A] = {
    wrap[String, A](
      col,
      _.isoMap(Iso(sizedIso.to, sizedIso.from)),
      _.copy(typeName = sizedStringType(sizedIso.size))
    )
  }

}
