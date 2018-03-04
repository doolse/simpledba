package io.doolse.simpledba2

import java.sql._

import shapeless.ops.record.Selector
import shapeless.{HList, LabelledGeneric, Witness}

case class PostgresColumn[A](sqlType: SQLType, getByIndex: (Int, ResultSet) => Option[A],
                             bind: (Int, AnyRef, Connection, PreparedStatement) => Unit) extends JDBCColumn[A]
{
  override def nullable: Boolean = ???

}

object PostgresColumn
{
  def temp[A](r: (Int, ResultSet) => Option[A],
              b: (Int, AnyRef, Connection, PreparedStatement) => Unit) = PostgresColumn[A](JDBCType.NVARCHAR, r, b)

  implicit val stringCol = temp[String]((i,rs) => Option(rs.getString(i)),
    (i,v,_,ps) => ps.setString(i, v.asInstanceOf[String]))
  implicit val intCol = temp[Int]((i, rs) =>
    Option(rs.getInt(i)).filterNot(_ => rs.wasNull),
    (i,v,_,ps) => ps.setInt(i, v.asInstanceOf[Int]))
  implicit val doubleCol : PostgresColumn[Double] = temp[Double](
    (i, rs) => Option(rs.getDouble(i)).filterNot(_ => rs.wasNull()),
    (i, v, _, ps) => ps.setDouble(i, v.asInstanceOf[Long]))
}

object PostgresMapper {
  val DefaultReserved = Set("user", "begin", "end")

  def escapeReserved(rw: Set[String])(s: String): String = {
    val lc = s.toLowerCase
    if (rw.contains(lc) || lc != s) '"' + s + '"' else s
  }

  val defaultEscapeReserved = escapeReserved(DefaultReserved) _
  val postGresConfig = JDBCSQLConfig(defaultEscapeReserved, defaultEscapeReserved)

  def table[T, Repr0 <: HList, KName <: Symbol, Key](tableName: String, gen: LabelledGeneric.Aux[T, Repr0],
                                           key: Witness.Aux[KName])(implicit sel: Selector.Aux[Repr0, KName, Key],
                                                                    allRelation: JDBCRelationBuilder[PostgresColumn, Repr0])
  : JDBCTable.Aux[T, Key, Repr0] = JDBCTable(tableName, gen, postGresConfig, allRelation, Seq(key.value.name),
    k => Seq(k.asInstanceOf[AnyRef]))
}
