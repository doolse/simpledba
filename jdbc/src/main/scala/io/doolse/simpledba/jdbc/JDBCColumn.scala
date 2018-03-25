package io.doolse.simpledba.jdbc

import java.sql._
import java.util.UUID

trait JDBCColumn {
  type A

  def sqlType: SQLType

  def nullable: Boolean

  def getByIndex: (Int, ResultSet) => Option[A]

  def bind: (Int, A, Connection, PreparedStatement) => Unit
}

class WrappedColumn[AA](wrapped: StdJDBCColumn[AA]) extends JDBCColumn
{
  override type A = AA

  override def sqlType: SQLType = wrapped.sqlType

  override def nullable: Boolean = wrapped.nullable

  override def getByIndex: (Int, ResultSet) => Option[A] = wrapped.getByIndex

  override def bind: (Int, A, Connection, PreparedStatement) => Unit = wrapped.bind
}

case class StdJDBCColumn[AA](sqlType: SQLType, nullable: Boolean,
                            getter: ResultSet => Int => AA,
                            bind: (Int, AA, Connection, PreparedStatement) => Unit) extends JDBCColumn
{
  override type A = AA

  override def getByIndex: (Int, ResultSet) => Option[AA] =
    (i,rs) => Option(getter(rs)(i)).filterNot(_ => rs.wasNull())

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
  implicit val stringCol =
    StdJDBCColumn[String](JDBCType.LONGNVARCHAR, false,
      _.getString,
    (i,v,_, ps) => ps.setString(i, v))

  implicit val intCol = StdJDBCColumn[Int](JDBCType.INTEGER, false,
    _.getInt,
    (i,v,_, ps) => ps.setInt(i, v))

  implicit val longCol = StdJDBCColumn[Long](JDBCType.BIGINT, false,
    _.getLong,
    (i,v,_, ps) => ps.setLong(i, v))

  implicit val boolCol = StdJDBCColumn[Boolean](JDBCType.BOOLEAN, false,
    _.getBoolean,
    (i,v,_, ps) => ps.setBoolean(i, v))

  implicit val doubleCol = StdJDBCColumn[Double](JDBCType.DOUBLE, false,
    _.getDouble,
    (i, v, _, ps) => ps.setDouble(i, v))

  val uuidCol = StdJDBCColumn[UUID](UuidSQLType, false,
    (rs) => (i) => rs.getObject(i).asInstanceOf[UUID],
    (i,v,_,ps) => ps.setObject(i, v))

}

