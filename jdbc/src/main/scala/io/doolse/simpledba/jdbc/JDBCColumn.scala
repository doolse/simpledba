package io.doolse.simpledba.jdbc

import java.sql._
import java.time.Instant
import java.util.UUID

import io.doolse.simpledba.{IsoAtom, PartialIsoAtom}

/**
  * Created by jolz on 12/03/17.
  */
sealed trait JDBCColumn[A] {
  def columnType: SQLType
  def byName: (JDBCSession, ResultSet, String) => Option[A]
  def bind: (JDBCSession, PreparedStatement, Int, A) => Unit
}

case class DialectColumn[A](columnType: SQLType) extends JDBCColumn[A] {
  val byName = (s: JDBCSession, r: ResultSet, n: String) => s.config.specialType(columnType).byName(r, n).asInstanceOf[Option[A]]
  val bind = (s: JDBCSession, ps: PreparedStatement, i: Int, a: A) => s.config.specialType(columnType).bind(ps, i, a.asInstanceOf[AnyRef])
}

case class StdJDBCColumn[A](columnType: SQLType, byName: (JDBCSession,ResultSet,String) => Option[A], bind: (JDBCSession,PreparedStatement,Int,A) => Unit) extends JDBCColumn[A]

case class WrappedColumn[S, A](wrapped: JDBCColumn[A], to: S => A, from: A => S) extends JDBCColumn[S] {
  val byName = (c: JDBCSession, r: ResultSet, n: String) => wrapped.byName(c, r, n).map(from)

  val bind = (c: JDBCSession, ps: PreparedStatement, i: Int, a: S) => wrapped.bind(c, ps, i, to(a))

  def columnType = wrapped.columnType
}

case class OptionalColumn[A](wrapped: JDBCColumn[A]) extends JDBCColumn[Option[A]]
{
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

  def direct[A](columnType: SQLType, byName: (ResultSet,String) => A, bindIndex: (PreparedStatement,Int,A) => Unit) =
    StdJDBCColumn[A](columnType, (s:JDBCSession,rs,n) => Option(byName(rs,n)), (s:JDBCSession,ps:PreparedStatement,i:Int,a:A) => bindIndex(ps,i,a))

  implicit val uuidColumn : JDBCColumn[UUID] = DialectColumn[UUID](UuidSQLType)
  implicit val stringColumn : JDBCColumn[String] = direct(JDBCType.LONGNVARCHAR, (r,n) => r.getString(n), (ps,i,a) => ps.setString(i, a))
  implicit val intColumn : JDBCColumn[Int] = direct(JDBCType.INTEGER, (r,n) => r.getInt(n), (ps,i,a) => ps.setInt(i, a))
  implicit val longColumn : JDBCColumn[Long] = direct(JDBCType.BIGINT, (r,n) => r.getLong(n), (ps,i,a) => ps.setLong(i, a))
  implicit val boolColumn : JDBCColumn[Boolean] = direct(JDBCType.BOOLEAN, (r,n) => r.getBoolean(n), (ps,i,a) => ps.setBoolean(i, a))
  implicit val shortColumn : JDBCColumn[Short] = direct(JDBCType.SMALLINT, (r,n) => r.getShort(n), (ps,i,a) => ps.setShort(i, a))
  implicit val floatColumn : JDBCColumn[Float] = direct(JDBCType.FLOAT, (r,n) => r.getFloat(n), (ps,i,a) => ps.setFloat(i, a))
  implicit val doubleColumn : JDBCColumn[Double] = direct(JDBCType.DOUBLE, (r,n) => r.getDouble(n), (ps,i,a) => ps.setDouble(i, a))
  val timestampColumn : JDBCColumn[Timestamp] = direct(JDBCType.TIMESTAMP, (r,n) => r.getTimestamp(n), (ps,i,a) => ps.setTimestamp(i, a))
  implicit val instantColumn : JDBCColumn[Instant] = WrappedColumn[Instant, Timestamp](timestampColumn, i => new Timestamp(i.toEpochMilli), t => Instant.ofEpochMilli(t.getTime))

  implicit def isoColumn[A, B](implicit iso: PartialIsoAtom[A, B], oc: JDBCColumn[B]): JDBCColumn[A] = WrappedColumn[A, B](oc, iso.from, iso.to)

  implicit def optionalCol[A](implicit col: JDBCColumn[A]): JDBCColumn[Option[A]] = OptionalColumn(col)


}