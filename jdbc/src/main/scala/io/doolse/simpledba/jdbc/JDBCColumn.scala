package io.doolse.simpledba.jdbc

import java.sql.{JDBCType, PreparedStatement, ResultSet, SQLType}
import java.util.UUID

import io.doolse.simpledba.IsoAtom

/**
  * Created by jolz on 12/03/17.
  */
sealed trait JDBCColumn[A] {
  def columnType: SQLType
  def byName(r: ResultSet, n: String): Option[A]
  def bind(ps: PreparedStatement, i: Int, a: A): Unit
}

case class StdJDBCColumn[A,A0](columnType: SQLType, _byName: (ResultSet,String) => A0, bindIndex: (PreparedStatement,Int,A0) => Unit, from: A0 => A, to: A => A0) extends JDBCColumn[A] {
  def byName(rs: ResultSet, n: String) = Option(_byName(rs, n)).map(from)
  def bind(ps: PreparedStatement, i: Int, a: A) = bindIndex(ps, i, to(a))
}

case class WrappedColumn[S, A](wrapped: JDBCColumn[A], to: S => A, from: A => S) extends JDBCColumn[S] {
  def byName(r: ResultSet, n: String): Option[S] = wrapped.byName(r, n).map(from)

  def bind(ps: PreparedStatement, i: Int, a: S) = wrapped.bind(ps, i, to(a))

  def columnType = wrapped.columnType
}

object JDBCColumn {
  val uuidSQLType = new SQLType {
    def getName = "UUID"
    def getVendorTypeNumber = 1
    def getVendor = "io.doolse"
  }
  implicit val uuidColumn : JDBCColumn[UUID] = StdJDBCColumn(uuidSQLType, (r,n) => r.getString(n), (ps, i, a:String) => ps.setString(i, a), UUID.fromString, _.toString())
  implicit val stringColumn : JDBCColumn[String] = StdJDBCColumn(JDBCType.LONGNVARCHAR, (r,n) => r.getString(n), (ps,i,a:String) => ps.setString(i, a), identity: (String => String), identity)
  implicit val intColumn : JDBCColumn[Int] = StdJDBCColumn(JDBCType.INTEGER, (r,n) => r.getInt(n), (ps,i,a:Int) => ps.setInt(i, a), identity: (Int => Int), identity)

  implicit def isoColumn[A, B](implicit iso: IsoAtom[A, B], oc: JDBCColumn[B]): JDBCColumn[A] = WrappedColumn[A, B](oc, iso.from, iso.to)

}