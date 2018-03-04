package io.doolse.simpledba2

import java.sql._

import shapeless.ops.record.{SelectAll, Selector}
import shapeless._

case class PostgresColumn[A](sqlType: SQLType, getByIndex: (Int, ResultSet) => Option[A],
                             bind: (Int, Any, Connection, PreparedStatement) => Unit) extends JDBCColumn[A]
{
  override def nullable: Boolean = ???

}

object PostgresColumn
{
  def temp[A](r: (Int, ResultSet) => Option[A],
              b: (Int, Any, Connection, PreparedStatement) => Unit) = PostgresColumn[A](JDBCType.NVARCHAR, r, b)

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

  def table[T, Repr <: HList, KName <: Symbol, Key <: HList](tableName: String, gen: LabelledGeneric.Aux[T, Repr],
                                           key: Witness.Aux[KName])(implicit ss: KeySubset.Aux[Repr, KName :: HNil, Key],
                                                                    allRelation: JDBCRelationBuilder[PostgresColumn, Repr])
  : JDBCTable.Aux[T, Repr, Key] = {
    val all = allRelation.apply()
    val keys = all.subset[KName :: HNil]
    JDBCTable(tableName, gen, postGresConfig, all, keys)
  }
}
