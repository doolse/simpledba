package io.doolse.simpledba.jdbc

import java.sql._

trait JDBCColumn {
  type A

  def sqlType: SQLType

  def nullable: Boolean

  def getByIndex: (Int, ResultSet) => Option[A]

  def bind: (Int, A, Connection, PreparedStatement) => Unit
}

case class StdJDBCColumn[AA](sqlType: SQLType, nullable: Boolean,
                            getter: (Int, ResultSet) => AA,
                            bind: (Int, AA, Connection, PreparedStatement) => Unit) extends JDBCColumn
{
  override type A = AA

  override def getByIndex: (Int, ResultSet) => Option[AA] =
    (i,rs) => Option(getter(i,rs)).filterNot(_ => rs.wasNull())

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

object JDBCColumn
{
  implicit val stringCol =
    StdJDBCColumn[String](JDBCType.LONGNVARCHAR, false,
      (i,rs) => rs.getString(i),
    (i,v,_, ps) => ps.setString(i, v))

  implicit val intCol = StdJDBCColumn[Int](JDBCType.INTEGER, false,
    (i, rs) => rs.getInt(i),
    (i,v,_, ps) => ps.setInt(i, v))

  implicit val longCol = StdJDBCColumn[Long](JDBCType.BIGINT, false,
    (i, rs) => rs.getLong(i),
    (i,v,_, ps) => ps.setLong(i, v))

  implicit val boolCol = StdJDBCColumn[Boolean](JDBCType.BOOLEAN, false,
    (i, rs) => rs.getBoolean(i),
    (i,v,_, ps) => ps.setBoolean(i, v))

  implicit val doubleCol = StdJDBCColumn[Double](JDBCType.DOUBLE, false,
    (i, rs) => rs.getDouble(i),
    (i, v, _, ps) => ps.setDouble(i, v))

}

