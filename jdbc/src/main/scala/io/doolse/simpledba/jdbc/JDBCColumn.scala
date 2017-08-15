package io.doolse.simpledba.jdbc

import java.sql.{Array => _, _}
import java.time.Instant
import java.util.UUID

import io.doolse.simpledba.{IsoAtom, PartialIsoAtom}

import scala.collection.mutable

/**
  * Created by jolz on 12/03/17.
  */
sealed trait JDBCColumn[A] {
  def columnType: SQLType
  def byName: (JDBCSession, ResultSet, String) => Option[A]
  def byIndex: (JDBCSession, ResultSet, Int) => Option[A]
  def bind: (JDBCSession, PreparedStatement, Int, A) => Unit
}

case class DialectColumn[A](columnType: SQLType) extends JDBCColumn[A] {
  val byName = (s: JDBCSession, r: ResultSet, n: String) => s.config.specialType(columnType).byName(r, n).asInstanceOf[Option[A]]
  val byIndex = (s:JDBCSession, rs: ResultSet, i: Int) => s.config.specialType(columnType).byIndex(rs, i).asInstanceOf[Option[A]]
  val bind = (s: JDBCSession, ps: PreparedStatement, i: Int, a: A) => s.config.specialType(columnType).bind(ps, i, a.asInstanceOf[AnyRef])
}

case class StdJDBCColumn[A](columnType: SQLType, byName: (JDBCSession,ResultSet,String) => Option[A], byIndex: (JDBCSession,ResultSet,Int) => Option[A], bind: (JDBCSession,PreparedStatement,Int,A) => Unit) extends JDBCColumn[A]

case class WrappedColumn[S, A](wrapped: JDBCColumn[A], to: S => A, from: A => S) extends JDBCColumn[S] {
  val byName = (c: JDBCSession, r: ResultSet, n: String) => wrapped.byName(c, r, n).map(from)

  val byIndex = (c: JDBCSession, r: ResultSet, n: Int) => wrapped.byIndex(c, r, n).map(from)

  val bind = (c: JDBCSession, ps: PreparedStatement, i: Int, a: S) => wrapped.bind(c, ps, i, to(a))

  def columnType = wrapped.columnType
}

case class OptionalColumn[A](wrapped: JDBCColumn[A]) extends JDBCColumn[Option[A]]
{
  val byIndex = (c: JDBCSession, r: ResultSet, i: Int) => Some(wrapped.byIndex(c, r, i))

  val byName = (c: JDBCSession, r: ResultSet, n: String) => Some(wrapped.byName(c, r, n))

  val bind = (c: JDBCSession, ps: PreparedStatement, i: Int, oa: Option[A]) => oa.fold(ps.setNull(i, java.sql.Types.NULL))((a:A) => wrapped.bind(c, ps, i, a))

  def columnType = wrapped.columnType
}

object JDBCColumn {
  case object UuidSQLType extends SQLType {
    def getName = "UUID"
    def getVendorTypeNumber = 1
    def getVendor = "io.doolse.simpledba"
  }

  case class ArraySQLType(elementType: SQLType) extends SQLType {
    def getName = "ARRAY"
    def getVendorTypeNumber = 2
    def getVendor = "io.doolse.simpledba"
  }

  def direct[A](columnType: SQLType, byName: ResultSet => String => A, byIndex: ResultSet => Int => A, bindIndex: (PreparedStatement,Int,A) => Unit) =
    StdJDBCColumn[A](columnType, (s:JDBCSession,rs,n) => Option(byName(rs)(n)).filterNot(_ => rs.wasNull),
      (s,rs,i) => Option(byIndex(rs)(i)).filterNot(_ => rs.wasNull),
      (s:JDBCSession,ps:PreparedStatement,i:Int,a:A) => bindIndex(ps,i,a))

  implicit val uuidColumn : JDBCColumn[UUID] = DialectColumn[UUID](UuidSQLType)
  implicit val stringColumn : JDBCColumn[String] = direct(JDBCType.LONGNVARCHAR, _.getString, _.getString, (ps,i,a) => ps.setString(i, a))
  implicit val intColumn : JDBCColumn[Int] = direct(JDBCType.INTEGER, _.getInt, _.getInt, (ps,i,a) => ps.setInt(i, a))
  implicit val longColumn : JDBCColumn[Long] = direct(JDBCType.BIGINT, _.getLong, _.getLong, (ps,i,a) => ps.setLong(i, a))
  implicit val boolColumn : JDBCColumn[Boolean] = direct(JDBCType.BOOLEAN, _.getBoolean, _.getBoolean, (ps,i,a) => ps.setBoolean(i, a))
  implicit val shortColumn : JDBCColumn[Short] = direct(JDBCType.SMALLINT, _.getShort, _.getShort, (ps,i,a) => ps.setShort(i, a))
  implicit val floatColumn : JDBCColumn[Float] = direct(JDBCType.FLOAT, _.getFloat, _.getFloat, (ps,i,a) => ps.setFloat(i, a))
  implicit val doubleColumn : JDBCColumn[Double] = direct(JDBCType.DOUBLE, _.getDouble, _.getDouble, (ps,i,a) => ps.setDouble(i, a))
  val timestampColumn : JDBCColumn[Timestamp] = direct(JDBCType.TIMESTAMP, _.getTimestamp, _.getTimestamp, (ps,i,a) => ps.setTimestamp(i, a))
  val sqlArrayColumn : JDBCColumn[java.sql.Array] = direct(JDBCType.ARRAY, _.getArray, _.getArray, (ps,i,a) => ps.setArray(i, a))

  implicit val instantColumn : JDBCColumn[Instant] = WrappedColumn[Instant, Timestamp](timestampColumn, i => new Timestamp(i.toEpochMilli), t => Instant.ofEpochMilli(t.getTime))

  implicit def isoColumn[A, B](implicit iso: PartialIsoAtom[A, B], oc: JDBCColumn[B]): JDBCColumn[A] = WrappedColumn[A, B](oc, iso.from, iso.to)

  implicit def optionalCol[A](implicit col: JDBCColumn[A]): JDBCColumn[Option[A]] = OptionalColumn(col)

  implicit def arrayCol[A](implicit col: JDBCColumn[A]): JDBCColumn[Vector[A]] = new JDBCColumn[Vector[A]] {
    def convertArray(s: JDBCSession)(a: java.sql.Array): Vector[A] = {
      val ars = a.getResultSet
      val b = mutable.Buffer[A]()
      while (ars.next()) {
        b += col.byIndex.apply(s, ars, 2).asInstanceOf[A]
      }
      b.toVector
    }
    val byName: (JDBCSession, ResultSet, String) => Option[Vector[A]] =
      (s,rs,n) => sqlArrayColumn.byName.apply(s, rs, n).map(convertArray(s))
    val byIndex: (JDBCSession, ResultSet, Int) => Option[Vector[A]] =
      (s,rs,i) => sqlArrayColumn.byIndex.apply(s, rs, i).map(convertArray(s))

    val wrappedCol = col.columnType
    val columnType: SQLType = ArraySQLType(wrappedCol)

    def bind: (JDBCSession, PreparedStatement, Int, Vector[A]) => Unit =
      (s, ps, i, v) => sqlArrayColumn.bind(s, ps, i, s.connection.createArrayOf(wrappedCol.getName, v.asInstanceOf[Vector[AnyRef]].toArray[AnyRef]))
  }


}