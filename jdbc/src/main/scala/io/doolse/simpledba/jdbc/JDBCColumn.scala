package io.doolse.simpledba.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet, _}
import java.util.UUID

import io.doolse.simpledba.Iso

trait JDBCColumn {
  type A

  def sqlType: SQLType

  def nullable: Boolean

  def read: (Int, ResultSet) => Option[A]

  def bind: (Int, A, Connection, PreparedStatement) => Unit
}

trait WrappedColumn[AA, C[_]] extends JDBCColumn
{
  val wrapped: StdJDBCColumn[AA]

  override type A = AA

  override def sqlType: SQLType = wrapped.sqlType

  override def nullable: Boolean = wrapped.nullable

  override def read: (Int, ResultSet) => Option[A] = wrapped.read

  override def bind: (Int, A, Connection, PreparedStatement) => Unit = wrapped.bind

  def mapped[A0]: StdJDBCColumn[A0] => C[A0]

  def isoMap[B](iso: Iso[B, AA]): C[B] = {
    mapped(wrapped.isoMap(iso))
  }
}

case class StdJDBCColumn[AA](sqlType: SQLType, nullable: Boolean, read: (Int, ResultSet) => Option[AA],
                            bind: (Int, AA, Connection, PreparedStatement) => Unit, cast: String => String) extends JDBCColumn
{
  override type A = AA

  def isoMap[B](iso: Iso[B, AA]): StdJDBCColumn[B] = {
    StdJDBCColumn[B](sqlType, nullable, (i,rs) => read(i, rs).map(iso.from), (i,a,c,rs) => bind(i, iso.to(a), c, rs), cast)
  }
}

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

case class SizedSQLType(subType: SQLType, size: Int) extends SQLType {
  def getName = "SIZED"
  def getVendorTypeNumber = 3
  def getVendor = "io.doolse.simpledba"
}

object StdJDBCColumn
{
  def colFromGetter[A](sql: SQLType, nullable: Boolean, getter: ResultSet => Int => A,
                       bind: (Int, A, Connection, PreparedStatement) => Unit): StdJDBCColumn[A] = {
    StdJDBCColumn[A](sql, nullable,
      (i, rs) => Option(getter(rs)(i)).filterNot(_ => rs.wasNull()), bind, identity)
  }

  implicit val stringCol =
    colFromGetter[String](JDBCType.LONGNVARCHAR, false,
      _.getString,
    (i,v,_, ps) => ps.setString(i, v))

  implicit val intCol = colFromGetter[Int](JDBCType.INTEGER, false,
    _.getInt,
    (i,v,_, ps) => ps.setInt(i, v))

  implicit val longCol = colFromGetter[Long](JDBCType.BIGINT, false,
    _.getLong,
    (i,v,_, ps) => ps.setLong(i, v))

  implicit val boolCol = colFromGetter[Boolean](JDBCType.BOOLEAN, false,
    _.getBoolean,
    (i,v,_, ps) => ps.setBoolean(i, v))

  implicit val doubleCol = colFromGetter[Double](JDBCType.DOUBLE, false,
    _.getDouble,
    (i, v, _, ps) => ps.setDouble(i, v))

  def objectCol[A](sqlType: SQLType, iso: Iso[A, Object]) : StdJDBCColumn[A] =
    colFromGetter[Object](sqlType, false, rs => i => rs.getObject(i), (i,v,_,ps) => ps.setObject(i, v)).isoMap(iso)

  val uuidCol = colFromGetter[UUID](UuidSQLType, false,
    rs => i => rs.getObject(i).asInstanceOf[UUID],
    (i,v,_,ps) => ps.setObject(i, v))

  implicit def optionalStdColumn[A](implicit wrapped: StdJDBCColumn[A]): StdJDBCColumn[Option[A]] = {
    def bind(i: Int, a: Option[A], c: Connection, ps: PreparedStatement) = a match {
      case None => ps.setNull(i, wrapped.sqlType.getVendorTypeNumber)
      case Some(v) => wrapped.bind(i, v, c, ps)
    }
    StdJDBCColumn[Option[A]](wrapped.sqlType, nullable = true,
      read = (i, rs) => Option(wrapped.read(i, rs)),
      bind = bind, cast = wrapped.cast)
  }

}

