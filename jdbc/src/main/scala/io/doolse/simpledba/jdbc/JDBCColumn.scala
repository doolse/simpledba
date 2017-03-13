package io.doolse.simpledba.jdbc

import java.sql.{JDBCType, PreparedStatement, ResultSet, SQLType}
import java.util.UUID

import io.doolse.simpledba.IsoAtom

/**
  * Created by jolz on 12/03/17.
  */
sealed trait JDBCColumn[A] {
  def columnType: SQLType
  def byName(c: JDBCSQLConfig, r: ResultSet, n: String): Option[A]
  def bind(c: JDBCSQLConfig, ps: PreparedStatement, i: Int, a: A): Unit
}

case class DialectColumn[A](columnType: SQLType) extends JDBCColumn[A] {
  def byName(c: JDBCSQLConfig, r: ResultSet, n: String): Option[A] = c.specialType(columnType).byName(r, n).asInstanceOf[Option[A]]
  def bind(c: JDBCSQLConfig, ps: PreparedStatement, i: Int, a: A): Unit = c.specialType(columnType).bind(ps, i, a.asInstanceOf[AnyRef])
}

case class StdJDBCColumn[A](columnType: SQLType, _byName: (ResultSet,String) => A, bindIndex: (PreparedStatement,Int,A) => Unit) extends JDBCColumn[A] {
  def byName(c: JDBCSQLConfig, rs: ResultSet, n: String) = Option(_byName(rs, n))
  def bind(c: JDBCSQLConfig, ps: PreparedStatement, i: Int, a: A) = bindIndex(ps, i, a)
}

case class WrappedColumn[S, A](wrapped: JDBCColumn[A], to: S => A, from: A => S) extends JDBCColumn[S] {
  def byName(c: JDBCSQLConfig, r: ResultSet, n: String): Option[S] = wrapped.byName(c, r, n).map(from)

  def bind(c: JDBCSQLConfig, ps: PreparedStatement, i: Int, a: S) = wrapped.bind(c, ps, i, to(a))

  def columnType = wrapped.columnType
}

object JDBCColumn {
  case object UuidSQLType extends SQLType {
    def getName = "UUID"
    def getVendorTypeNumber = 1
    def getVendor = "io.doolse.simpledba"
  }

  def direct[A](columnType: SQLType, byName: (ResultSet,String) => A, bindIndex: (PreparedStatement,Int,A) => Unit) =
    StdJDBCColumn[A](columnType, byName, bindIndex)

  implicit val uuidColumn : JDBCColumn[UUID] = DialectColumn[UUID](UuidSQLType)
  implicit val stringColumn : JDBCColumn[String] = direct(JDBCType.LONGNVARCHAR, (r,n) => r.getString(n), (ps,i,a) => ps.setString(i, a))
  implicit val intColumn : JDBCColumn[Int] = direct(JDBCType.INTEGER, (r,n) => r.getInt(n), (ps,i,a) => ps.setInt(i, a))
  implicit val longColumn : JDBCColumn[Long] = direct(JDBCType.BIGINT, (r,n) => r.getLong(n), (ps,i,a) => ps.setLong(i, a))
  implicit val boolColumn : JDBCColumn[Boolean] = direct(JDBCType.BOOLEAN, (r,n) => r.getBoolean(n), (ps,i,a) => ps.setBoolean(i, a))
  implicit val shortColumn : JDBCColumn[Short] = direct(JDBCType.SMALLINT, (r,n) => r.getShort(n), (ps,i,a) => ps.setShort(i, a))
  implicit val floatColumn : JDBCColumn[Float] = direct(JDBCType.FLOAT, (r,n) => r.getFloat(n), (ps,i,a) => ps.setFloat(i, a))
  implicit val doubleColumn : JDBCColumn[Double] = direct(JDBCType.DOUBLE, (r,n) => r.getDouble(n), (ps,i,a) => ps.setDouble(i, a))

  implicit def isoColumn[A, B](implicit iso: IsoAtom[A, B], oc: JDBCColumn[B]): JDBCColumn[A] = WrappedColumn[A, B](oc, iso.from, iso.to)

}