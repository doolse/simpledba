package io.doolse.simpledba2.jdbc

import java.sql._

case class PostgresColumn[AA](sqlType: SQLType, getByIndex: (Int, ResultSet) => Option[AA],
                             bind: (Int, AA, Connection, PreparedStatement) => Unit) extends JDBCColumn
{
  type A = AA
  override def nullable: Boolean = ???

}

object PostgresColumn
{
  def temp[A](r: (Int, ResultSet) => Option[A],
              b: (Int, A, Connection, PreparedStatement) => Unit) = PostgresColumn[A](JDBCType.NVARCHAR, r, b)

  implicit val stringCol = temp[String]((i,rs) => Option(rs.getString(i)),
    (i,v,_,ps) => ps.setString(i, v))
  implicit val intCol = temp[Int]((i, rs) =>
    Option(rs.getInt(i)).filterNot(_ => rs.wasNull),
    (i,v,_,ps) => ps.setInt(i, v))
  implicit val longCol = temp[Long]((i, rs) =>
    Option(rs.getLong(i)).filterNot(_ => rs.wasNull),
    (i,v,_,ps) => ps.setLong(i, v))
  implicit val boolCol = temp[Boolean]((i, rs) =>
    Option(rs.getBoolean(i)).filterNot(_ => rs.wasNull),
    (i,v,_,ps) => ps.setBoolean(i, v))
  implicit val doubleCol : PostgresColumn[Double] = temp[Double](
    (i, rs) => Option(rs.getDouble(i)).filterNot(_ => rs.wasNull()),
    (i, v, _, ps) => ps.setDouble(i, v))
}

object PostgresMapper {
  val DefaultReserved = Set("user", "begin", "end")

  def escapeReserved(rw: Set[String])(s: String): String = {
    val lc = s.toLowerCase
    if (rw.contains(lc) || lc != s) '"' + s + '"' else s
  }

  val defaultEscapeReserved = escapeReserved(DefaultReserved) _
  val postGresConfig = new JDBCSQLConfig(defaultEscapeReserved, defaultEscapeReserved)
  {
    type C[A] = PostgresColumn[A]
  }

}
