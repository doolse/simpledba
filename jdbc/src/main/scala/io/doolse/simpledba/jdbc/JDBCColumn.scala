package io.doolse.simpledba.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet, _}
import java.time.Instant
import java.util.UUID

import io.doolse.simpledba.Iso

import scala.collection.mutable
import scala.reflect.ClassTag

case class BoundValue(expr: SQLExpression, bind: (Int, Connection, PreparedStatement) => Int, value: Any)

trait JDBCColumn[A] {

  def jdbcType: JDBCType

  def columnType: ColumnType

  def bindValue: A => BoundValue

  def bindUpdate: (A, A) => Option[BoundValue]

  def read: (Int, ResultSet) => Option[A]
}

trait WrappedColumn[A] extends JDBCColumn[A] {
  val wrapped: StdJDBCColumn[A]

  override def jdbcType: JDBCType = wrapped.jdbcType

  override def read: (Int, ResultSet) => Option[A] = wrapped.read

  override def bindValue: A => BoundValue = v => BoundValue(Parameter(columnType), (i,c,ps) => {
    wrapped.bind(v)(i, c, ps)
    i+1
  }, v)

  override def bindUpdate: (A, A) => Option[BoundValue] =
    (o, n) =>
      if (o == n) None
      else {
        Some(bindValue(n))
    }

}

case class SizedIso[A, B](size: Int, to: A => B, from: B => A)

case class StdJDBCColumn[A](
    jdbcType: JDBCType,
    read: (Int, ResultSet) => Option[A],
    bind: A => (Int, Connection, PreparedStatement) => Unit
) {
  def isoMap[B](iso: Iso[B, A]): StdJDBCColumn[B] = {

    StdJDBCColumn[B](jdbcType, (i, rs) => read(i, rs).map(iso.from), b => bind(iso.to(b)))
  }
}

object StdJDBCColumn {
  def instantIso: Iso[Instant, java.sql.Timestamp] = Iso(java.sql.Timestamp.from, _.toInstant)

  def colFromGetter[A](
      sql: JDBCType,
      getter: ResultSet => Int => A,
      bind: A => (Int, Connection, PreparedStatement) => Unit
  ): StdJDBCColumn[A] = {
    StdJDBCColumn[A](sql, (i, rs) => Option(getter(rs)(i)).filterNot(_ => rs.wasNull()), bind)
  }

  val stringCol =
    colFromGetter[String](JDBCType.LONGVARCHAR, _.getString, v => (i, _, ps) => ps.setString(i, v))

  val intCol = colFromGetter[Int](JDBCType.INTEGER, _.getInt, v => (i, _, ps) => ps.setInt(i, v))

  val shortCol = colFromGetter[Short](JDBCType.SMALLINT, _.getShort, v => (i, _, ps) => ps.setShort(i, v))

  val longCol = colFromGetter[Long](JDBCType.BIGINT, _.getLong, v => (i, _, ps) => ps.setLong(i, v))

  val boolCol =
    colFromGetter[Boolean](JDBCType.BOOLEAN, _.getBoolean, v => (i, _, ps) => ps.setBoolean(i, v))

  val floatCol =
    colFromGetter[Float](JDBCType.FLOAT, _.getFloat, v => (i, _, ps) => ps.setFloat(i, v))

  val doubleCol =
    colFromGetter[Double](JDBCType.DOUBLE, _.getDouble, v => (i, _, ps) => ps.setDouble(i, v))

  val timestampCol = colFromGetter[java.sql.Timestamp](
    JDBCType.TIMESTAMP,
    _.getTimestamp,
    v => (i, _, ps) => ps.setTimestamp(i, v)
  )

  val instantCol = timestampCol.isoMap(instantIso)

  def objectCol[A](jdbcType: JDBCType, iso: Iso[A, Object]): StdJDBCColumn[A] =
    colFromGetter[Object](
      jdbcType,
      rs => i => rs.getObject(i),
      v => (i, _, ps) => ps.setObject(i, v)
    ).isoMap(iso)

  def uuidCol(jdbcType: JDBCType) =
    colFromGetter[UUID](
      jdbcType,
      rs => i => rs.getObject(i).asInstanceOf[UUID],
      v => (i, _, ps) => ps.setObject(i, v)
    )

  def arrayCol[A: ClassTag](typeName: String, inner: StdJDBCColumn[A]) =
    colFromGetter[scala.Array[A]](
      JDBCType.ARRAY,
      rs =>
        i => {
          val arrs = rs.getArray(i).getResultSet
          val buf  = mutable.Buffer[A]()
          while (arrs.next()) {
            buf += inner.read(2, arrs).get
          }
          buf.toArray[A]
      },
      v =>
        (i, con, ps) =>
          ps.setArray(i, con.createArrayOf(typeName, v.asInstanceOf[scala.Array[AnyRef]]))
    )

  def optionalColumn[A](wrapped: StdJDBCColumn[A]): StdJDBCColumn[Option[A]] = {
    def doSetNull =
      (i: Int, _: Connection, ps: PreparedStatement) =>
        ps.setNull(i, wrapped.jdbcType.getVendorTypeNumber)
    StdJDBCColumn[Option[A]](
      wrapped.jdbcType,
      read = (i, rs) => Option(wrapped.read(i, rs)),
      bind = {
        case None =>
          (i, con, ps) =>
            ps.setNull(i, wrapped.jdbcType.getVendorTypeNumber)
        case Some(v) => wrapped.bind(v)
      }
    )
  }

}
