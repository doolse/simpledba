package io.doolse.simpledba2

import java.sql._

import shapeless.ops.record.{SelectAll, Selector}
import shapeless._

case class PostgresColumn[A](sqlType: SQLType, getByIndex: (Int, ResultSet) => Option[Any],
                             bind: (Int, Any, Connection, PreparedStatement) => Unit) extends JDBCColumn
{
  override def nullable: Boolean = ???

}

object PostgresColumn
{
  def temp[A](r: (Int, ResultSet) => Option[Any],
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

  def embedded[T, GRepr <: HList, Repr0 <: HList](gen: LabelledGeneric.Aux[T, GRepr])(
    implicit columns: ColumnBuilder.Aux[PostgresColumn, GRepr, Repr0]):
    ColumnBuilder.Aux[PostgresColumn, T, Repr0] = new ColumnBuilder[PostgresColumn, T] {
      type Repr = Repr0
      def apply() = columns().isomap(gen.from, gen.to)
  }

  def table[T, GRepr <: HList, Repr <: HList, KName <: Symbol, Key <: HList]
  (gen: LabelledGeneric.Aux[T, GRepr], tableName: String, key: Witness.Aux[KName])(
    implicit
    allRelation: ColumnBuilder.Aux[PostgresColumn, GRepr, Repr],
    ss: KeySubset.Aux[Repr, KName :: HNil, Key])
  : JDBCTable.Aux[T, Repr, Key] = {
    val all = allRelation.apply().isomap(gen.from, gen.to)
    val keys = all.subset[KName :: HNil]
    JDBCTable(tableName, postGresConfig, all, keys)
  }
}
